/*********************************************************************************************************************
 *  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
 *                                                                                                                    *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    *
 *  with the License. A copy of the License is located at                                                             *
 *                                                                                                                    *
 *      http://www.apache.org/licenses/LICENSE-2.0                                                                    *
 *                                                                                                                    *
 *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES *
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
 *  and limitations under the License.                                                                                *
 *********************************************************************************************************************/

/**
 * @author Solution Builders
 */

'use strict';

import * as cdk from '@aws-cdk/core';
import * as sns from '@aws-cdk/aws-sns';
import * as s3 from '@aws-cdk/aws-s3';
import * as lambda from '@aws-cdk/aws-lambda';
import * as logs from '@aws-cdk/aws-logs';
import * as iam from '@aws-cdk/aws-iam';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';
import * as glue from '@aws-cdk/aws-glue';
import * as iamSec from './iam-security';
import * as path from 'path';
import {GlueDataCatalog} from "./glue-data-catalog";
import {DynamoDataCatalog} from "./ddb-data-catalog";
import {StageTwoOrchestrator} from "./stage-two-orchestrator";

export interface StageTwoProps {
    readonly stagingBucket: s3.IBucket;
    readonly iamSecurity: iamSec.IamSecurity;
    readonly glueDataCatalog: GlueDataCatalog;
    readonly dynamoDataCatalog: DynamoDataCatalog;
    readonly glacierSourceVault: string;
    readonly glacierRetrievalTier: string;
    readonly archiveNotificationTopic: sns.ITopic;
    readonly sendAnonymousStats: lambda.IFunction;
}

export class StageTwo extends cdk.Construct {
    public readonly stageTwoOrchestrator: sfn.StateMachine;

    constructor(scope: cdk.Construct, id: string, props: StageTwoProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Deploy Glue Job Script
        const deployGlueJobScript = new lambda.Function(this, 'DeployGlueJobScript', {
            functionName: `${cdk.Aws.STACK_NAME}-deployGlueJobScript`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            timeout: cdk.Duration.minutes(1),
            memorySize: 128,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/deployGlueJobScript')),
            environment:
                {
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                }
        });
        props.stagingBucket.grantWrite(deployGlueJobScript);

        const deployGlueJobScriptTrigger = new cdk.CustomResource(this, 'deployGlueJobScriptTrigger',
            {
                serviceToken: deployGlueJobScript.functionArn
            });

        // -------------------------------------------------------------------------------------------
        // Request Archives
        const requestArchives = new lambda.Function(this, 'RequestArchives', {
            functionName: `${cdk.Aws.STACK_NAME}-requestArchives`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/requestArchives')),
            environment:
                {
                    AWS_NODEJS_CONNECTION_REUSE_ENABLED: "1",
                    SNS_TOPIC: props.archiveNotificationTopic.topicArn,
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                    TIER: props.glacierRetrievalTier,
                    STATUS_TABLE: props.dynamoDataCatalog.statusTable.tableName,
                    VAULT: props.glacierSourceVault,
                    DATABASE: props.glueDataCatalog.inventoryDatabase.databaseName,
                    ATHENA_WORKGROUP: props.glueDataCatalog.athenaWorkgroup.name,
                    PARTITIONED_INVENTORY_TABLE: props.glueDataCatalog.partitionedInventoryTable.tableName
                }
        });

        props.stagingBucket.grantReadWrite(requestArchives);
        requestArchives.addToRolePolicy(iamSec.IamSecurity.athenaPermissions([
            props.glueDataCatalog.inventoryDatabase.catalogArn,
            props.glueDataCatalog.inventoryDatabase.databaseArn,
            `arn:aws:athena:*:${cdk.Aws.ACCOUNT_ID}:workgroup/${props.glueDataCatalog.athenaWorkgroup.name}`,
            props.glueDataCatalog.partitionedInventoryTable.tableArn
        ]));
        props.dynamoDataCatalog.statusTable.grantReadWriteData(requestArchives);
        requestArchives.addToRolePolicy(iamSec.IamSecurity.glacierPermitOperations(props.glacierSourceVault));

        // -------------------------------------------------------------------------------------------
        // Glue Partitioning Job
        const glueRole = new iam.Role(this, 'GluePartitionRole', {
            roleName: `${cdk.Aws.STACK_NAME}-glue-repartition-role`,
            assumedBy: new iam.ServicePrincipal('glue.amazonaws.com')
        });

        props.stagingBucket.grantReadWrite(glueRole);
        glueRole.addToPolicy(iamSec.IamSecurity.athenaPermissions(
            [
                props.glueDataCatalog.inventoryDatabase.catalogArn,
                props.glueDataCatalog.inventoryDatabase.databaseArn,
                `arn:aws:athena:*:${cdk.Aws.ACCOUNT_ID}:workgroup/${props.glueDataCatalog.athenaWorkgroup.name}`,
                props.glueDataCatalog.inventoryTable.tableArn,
                props.glueDataCatalog.filelistTable.tableArn,
                props.glueDataCatalog.partitionedInventoryTable.tableArn
            ]
        ));

        glueRole.addToPolicy(
            new iam.PolicyStatement({
                sid: 'allowLogging',
                effect: iam.Effect.ALLOW,
                actions: [
                    'logs:*'
                ],
                resources:
                    [
                        `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/cdk-glueJob:**`,
                        `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/*:**`
                    ]
            }));

        const glueJobName = `${cdk.Aws.STACK_NAME}-glacier-refreezer`;
        const glueJob = new glue.CfnJob(this, 'GlueRepartitionJob',
            {
                name: glueJobName,
                description: 'To repartition the inventory table',
                maxCapacity: 5,
                glueVersion: '2.0',
                maxRetries: 0,
                executionProperty:
                    {maxConcurrentRuns: 1},
                command:
                    {
                        name: 'glueetl',
                        scriptLocation: `s3://${props.stagingBucket.bucketName}/glue/partition-inventory.py`,
                    },
                defaultArguments:
                    {
                        '--job-bookmark-option': 'job-bookmark-disable',
                        '--enable-continuous-cloudwatch-log': 'true',
                        '--enable-continuous-log-filter': 'false',
                        '--DATABASE': props.glueDataCatalog.inventoryDatabase.databaseName,
                        '--INVENTORY_TABLE': props.glueDataCatalog.inventoryTable.tableName,
                        '--FILENAME_TABLE': props.glueDataCatalog.filelistTable.tableName,
                        '--OUTPUT_TABLE': props.glueDataCatalog.partitionedInventoryTable.tableName,
                        '--STAGING_BUCKET': props.stagingBucket.bucketName
                    },
                role: glueRole.roleArn
            });

        this.stageTwoOrchestrator = new StageTwoOrchestrator(this, 'Stepfunctions', {
            dynamoDataCatalog: props.dynamoDataCatalog,
            glueDataCatalog: props.glueDataCatalog,
            sendAnonymousStats: props.sendAnonymousStats,
            glueJobName,
            requestArchives
        }).stateMachine;
    }
}