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
        // Deploy Glue Script Helper
        const deployGlueScriptHelper = new lambda.Function(this, 'DeployGlueScriptHelper', {
            functionName: `${cdk.Aws.STACK_NAME}-deployGlueScriptHelper`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            timeout: cdk.Duration.minutes(1),
            memorySize: 128,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/deployGlueScriptHelper')),
            environment:
                {
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                }
        });

        props.stagingBucket.grantWrite(deployGlueScriptHelper);

        // const glueCustomResource = new cdk.CustomResource(this, 'deployGlueScriptTrigger',
        //     {
        //         serviceToken: `${cdk.Fn.getAtt((deployGlueScriptHelper.node.defaultChild as lambda.CfnFunction).logicalId, 'Arn')}`
        //     });

        const glueCustomResource = new cdk.CustomResource(this, 'deployGlueScriptTrigger',
            {
                serviceToken: deployGlueScriptHelper.functionArn
            });

        // -------------------------------------------------------------------------------------------
        // Get Partition Count
        const getPartitionCount = new lambda.Function(this, 'GetPartitionCount', {
            functionName: `${cdk.Aws.STACK_NAME}-getPartitionCount`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 128,
            timeout: cdk.Duration.minutes(15),
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/getPartitionCount')),
            environment:
                {
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                    DATABASE: props.glueDataCatalog.inventoryDatabase.databaseName,
                    PARTITIONED_INVENTORY_TABLE: props.glueDataCatalog.partitionedInventoryTable.tableName,
                    ATHENA_WORKGROUP: props.glueDataCatalog.athenaWorkgroup.name
                }
        });

        props.stagingBucket.grantReadWrite(getPartitionCount);
        getPartitionCount.addToRolePolicy(iamSec.IamSecurity.athenaPermissions([
            props.glueDataCatalog.inventoryDatabase.catalogArn,
            props.glueDataCatalog.inventoryDatabase.databaseArn,
            `arn:aws:athena:*:${cdk.Aws.ACCOUNT_ID}:workgroup/${props.glueDataCatalog.athenaWorkgroup.name}`,
            props.glueDataCatalog.partitionedInventoryTable.tableArn
        ]));

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
        // Glue Repartition Job
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

        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator :: Tasks

        const taskRunGlueJob = new tasks.GlueStartJobRun(this, 'Run Glue Partitioning', {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            glueJobName: glueJobName
        });

        const taskGetPartitionCount = new tasks.LambdaInvoke(this, 'Get Partition Count', {
            lambdaFunction: getPartitionCount,
            outputPath: '$.Payload'
        });

        const taskRequestArchives = new tasks.LambdaInvoke(this, 'Request Archives Retrieval', {
            lambdaFunction: requestArchives,
            outputPath: '$.Payload'
        });

        taskRequestArchives.addRetry({
            maxAttempts: 10000,
            backoffRate: 1,
            interval: cdk.Duration.seconds(15)
        });

        const taskStartInventoryQuery = new tasks.AthenaStartQueryExecution(this, 'Start Inventory Query', {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            queryString: `SELECT COUNT(1) AS archiveCount, COALESCE(SUM(size),0) AS vaultSize FROM "${props.glueDataCatalog.inventoryTable.tableName}";`,
            queryExecutionContext: {
                databaseName: props.glueDataCatalog.inventoryDatabase.databaseName
            },
            workGroup: props.glueDataCatalog.athenaWorkgroup.name,
            outputPath: '$.QueryExecution'
        });

        const taskGetInventoryResults = new tasks.AthenaGetQueryResults(this, 'Get Inventory Results', {
            queryExecutionId: sfn.JsonPath.stringAt('$.QueryExecutionId'),
            outputPath: '$.ResultSet.Rows[1]'
        });

        const taskStartPartitionedQuery = new tasks.AthenaStartQueryExecution(this, 'Start Partitioned Query', {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            queryString: `SELECT COUNT(1) AS archiveCount, COALESCE(SUM(size),0) AS vaultSize FROM "${props.glueDataCatalog.partitionedInventoryTable.tableName}";`,
            queryExecutionContext: {
                databaseName: props.glueDataCatalog.inventoryDatabase.databaseName
            },
            workGroup: props.glueDataCatalog.athenaWorkgroup.name,
            outputPath: '$.QueryExecution'
        });

        const taskGetPartitionedResults = new tasks.AthenaGetQueryResults(this, 'Get Partitioned Results', {
            queryExecutionId: sfn.JsonPath.stringAt('$.QueryExecutionId'),
            outputPath: '$.ResultSet.Rows[1]'
        });

        const taskSubmitStatistics = new tasks.LambdaInvoke(this, 'Submit Anonymous Statistics', {
            lambdaFunction: props.sendAnonymousStats,
            inputPath: '$.inventoryTable'
        });

        const taskUpdateMetricTable = new tasks.DynamoPutItem(this, 'Update Metric Table', {
            item: {
                pk: tasks.DynamoAttributeValue.fromString('totalRecordCount'),
                value: tasks.DynamoAttributeValue.numberFromString(sfn.JsonPath.stringAt('$.inventoryTable.archiveCount'))
            },
            table: props.dynamoDataCatalog.metricTable,
            resultPath: '$.putItemResult'
        });

        const taskMergeQueryResults = new sfn.Pass(this, 'Merge Query Results', {
            parameters: {
                inventoryTable: {
                    archiveCount: sfn.JsonPath.numberAt('$[0].Data[0].VarCharValue'),
                    vaultSize: sfn.JsonPath.numberAt('$[0].Data[1].VarCharValue')
                },
                partitionedTable: {
                    archiveCount: sfn.JsonPath.numberAt('$[1].Data[0].VarCharValue'),
                    vaultSize: sfn.JsonPath.numberAt('$[1].Data[1].VarCharValue')
                }
            },
            resultPath: '$',
        });

        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator :: Graph
        const stepParallelQuery = new sfn.Parallel(this, 'Parallel Queries', {})
            .branch(taskStartInventoryQuery.next(taskGetInventoryResults))
            .branch(taskStartPartitionedQuery.next(taskGetPartitionedResults));

        const stepParallelPartition = new sfn.Parallel(this, 'Parallel Partitioning and Stats update', {})
            .branch(taskRunGlueJob)
            .branch(taskUpdateMetricTable.next(taskSubmitStatistics));

        const taskPartitionStatusCheck =
                new sfn.Choice(this, 'Partitioning Required ?')
                .when(sfn.Condition.stringEqualsJsonPath('$.inventoryTable.archiveCount','$.partitionedTable.archiveCount'), taskGetPartitionCount)
                .otherwise(stepParallelPartition.next(taskGetPartitionCount));

        const taskInventoryCheck =         
                new sfn.Choice(this, 'Check Inventory State')
                .when(
                    sfn.Condition.stringEquals('$.inventoryTable.archiveCount', "0"), 
                    new sfn.Fail(this,'FAIL: Inventory Empty',{
                        error: 'Vault Inventory Table is empty. Has it been downloaded?'
                    }))
                .when(sfn.Condition.and(
                        sfn.Condition.not(sfn.Condition.stringEquals('$.partitionedTable.archiveCount', "0")), 
                        sfn.Condition.not(sfn.Condition.stringEqualsJsonPath('$.partitionedTable.archiveCount', '$.inventoryTable.archiveCount'))),
                    new sfn.Fail(this,'FAIL: Inventory-Partitioned Mismatch',{
                        error: 'Inventory and Partitioned table counts are greater than 0 and do not match. Cannot proceed.'
                }))
                .otherwise(taskPartitionStatusCheck);

        taskGetPartitionCount
            .next(taskRequestArchives)
            .next(new sfn.Choice(this, 'Complete?')
                .when(sfn.Condition.booleanEquals('$.isComplete', false), taskRequestArchives)
                .otherwise(new sfn.Succeed(this, 'Success')));

        const graphDefinition = stepParallelQuery
                .next(taskMergeQueryResults)
                .next(taskInventoryCheck);

        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator :: Permissions
        const stageTwoOrechestratorRole = new iam.Role(this, 'stageTwoOrchestratorRole', {
            assumedBy: new iam.ServicePrincipal('states.amazonaws.com')
        });

        getPartitionCount.grantInvoke(stageTwoOrechestratorRole);
        requestArchives.grantInvoke(stageTwoOrechestratorRole);
        getPartitionCount.grantInvoke(stageTwoOrechestratorRole);
        props.sendAnonymousStats.grantInvoke(stageTwoOrechestratorRole);
        props.dynamoDataCatalog.metricTable.grantWriteData(stageTwoOrechestratorRole);

        stageTwoOrechestratorRole.addToPolicy(iamSec.IamSecurity.athenaPermissions(
            [
                props.glueDataCatalog.inventoryDatabase.catalogArn,
                props.glueDataCatalog.inventoryDatabase.databaseArn,
                `arn:aws:athena:*:${cdk.Aws.ACCOUNT_ID}:workgroup/${props.glueDataCatalog.athenaWorkgroup.name}`,
                props.glueDataCatalog.inventoryTable.tableArn,
                props.glueDataCatalog.partitionedInventoryTable.tableArn
            ]
        ));

        stageTwoOrechestratorRole.addToPolicy(new iam.PolicyStatement({
            sid: 'allowGlueJobRun',
            effect: iam.Effect.ALLOW,
            actions: [
                'glue:StartJobRun',
                'glue:GetJobRun',
                'glue:GetJobRuns'
            ],
            resources: [`arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:job/${glueJobName}`]
        }));

        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator :: StepFunction
        const stageTwoSfLogGroup = logs.LogGroup.fromLogGroupName(this, 'stageTwoOrchestrator', `/aws/states/${cdk.Aws.STACK_NAME}-stageTwoOrchestrator`);

        this.stageTwoOrchestrator = new sfn.StateMachine(this, 'StageTwoOrchestrator', {
            stateMachineName: `${cdk.Aws.STACK_NAME}-stageTwoOrchestrator`,
            stateMachineType: sfn.StateMachineType.STANDARD,
            definition: graphDefinition,
            role: stageTwoOrechestratorRole,
            logs: {
                destination: stageTwoSfLogGroup,
                level: sfn.LogLevel.ALL,
            }
        });
        (this.stageTwoOrchestrator.node.defaultChild as sfn.CfnStateMachine).overrideLogicalId(`stageTwoOrchestrator`);

    }
}