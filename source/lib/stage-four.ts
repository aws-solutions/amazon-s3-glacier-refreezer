/*********************************************************************************************************************
 *  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
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
import * as sqs from '@aws-cdk/aws-sqs';
import * as s3 from '@aws-cdk/aws-s3';
import * as iamSec from './iam-permissions';
import * as lambda from '@aws-cdk/aws-lambda';
import * as path from 'path';
import * as dynamo from "@aws-cdk/aws-dynamodb";
import * as iam from "@aws-cdk/aws-iam";
import { SqsEventSource } from '@aws-cdk/aws-lambda-event-sources';
import { CfnNagSuppressor } from "./cfn-nag-suppressor";

export interface StageFourProps {
    readonly stagingBucket: s3.IBucket;
    readonly destinationBucket: string,
    readonly destinationStorageClass: string,
    readonly treehashCalcQueue: sqs.IQueue;
    readonly archiveNotificationQueue: sqs.IQueue;
    readonly statusTable: dynamo.ITable;
    readonly metricTable: dynamo.ITable;
}

export class StageFour extends cdk.Construct {

    constructor(scope: cdk.Construct, id: string, props: StageFourProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Get Destination S3 bucket
        const destinationBucket = s3.Bucket.fromBucketName(this, 'destinationBucket', props.destinationBucket)

        // -------------------------------------------------------------------------------------------
        // copyToDestinationBucketQueue Queue
        const copyToDestinationBucketQueue = new sqs.Queue(this, 'destination-copy-queue',
            {
                queueName: `${cdk.Aws.STACK_NAME}-destination-copy-queue`,
                retentionPeriod: cdk.Duration.days(14),
                visibilityTimeout: cdk.Duration.seconds(905)
            }
        );
        CfnNagSuppressor.addSuppression(copyToDestinationBucketQueue, 'W48', 'Non sensitive metadata - encryption is not required and cost inefficient');
        copyToDestinationBucketQueue.addToResourcePolicy(iamSec.IamPermissions.sqsDenyInsecureTransport(copyToDestinationBucketQueue));

        // -------------------------------------------------------------------------------------------
        // Calculate Treehash and verify SHA256TreeHash Glacier == SHA256TreeHash S3
        const calculateTreehashRole = new iam.Role(this, 'CalculateTreehashRole', {
            assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
        });

        const calculateTreehashRolePolicy = new iam.Policy(this, 'CalculateTreehashRolePolicy', {
            statements: [
                iamSec.IamPermissions.lambdaLogGroup(`${cdk.Aws.STACK_NAME}-calculateTreehash`),
                new iam.PolicyStatement({
                    sid: 'allowPutObject',
                    effect: iam.Effect.ALLOW,
                    actions: [
                        's3:PutObject'
                    ],
                    resources: [`${destinationBucket.bucketArn}/*`]
                }),
                new iam.PolicyStatement({
                    sid: 'allowListBucket',
                    effect: iam.Effect.ALLOW,
                    actions: [
                        's3:ListBucket'
                    ],
                    resources: [`${destinationBucket.bucketArn}`]
                }),
            ]
        });
        calculateTreehashRolePolicy.attachToRole(calculateTreehashRole);

        props.stagingBucket.grantReadWrite(calculateTreehashRole);
        props.statusTable.grantReadWriteData(calculateTreehashRole);
        props.metricTable.grantReadWriteData(calculateTreehashRole);
        props.archiveNotificationQueue.grantSendMessages(calculateTreehashRole);
        copyToDestinationBucketQueue.grantSendMessages(calculateTreehashRole);

        const defaultCalculateTreehashPolicy = calculateTreehashRole.node.findChild('DefaultPolicy').node.defaultChild as cdk.CfnResource;
        CfnNagSuppressor.addCfnSuppression(defaultCalculateTreehashPolicy, 'W76', 'Policy is auto-generated by CDK');

        const calculateTreehash = new lambda.Function(this, 'calculateTreehash', {
            functionName: `${cdk.Aws.STACK_NAME}-calculateTreehash`,
            runtime: lambda.Runtime.NODEJS_14_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 55,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/calculateTreehash')),
            role: calculateTreehashRole,
            environment:
            {
                DESTINATION_BUCKET: props.destinationBucket,
                STORAGE_CLASS: props.destinationStorageClass,
                STAGING_BUCKET: props.stagingBucket.bucketName,
                STAGING_BUCKET_PREFIX: 'stagingdata',
                STATUS_TABLE: props.statusTable.tableName,
                METRIC_TABLE: props.metricTable.tableName,
                SQS_ARCHIVE_NOTIFICATION: props.archiveNotificationQueue.queueName,
                SQS_COPY_TO_DESTINATION_NOTIFICATION: copyToDestinationBucketQueue.queueName
            }
        });
        calculateTreehash.addEventSource(new SqsEventSource(props.treehashCalcQueue, { batchSize: 1 }));
        CfnNagSuppressor.addLambdaSuppression(calculateTreehash);

        // -------------------------------------------------------------------------------------------
        // Copy archive from Staging to Destination and delete Staging thereafter
        const copyToDestinationBucket = new lambda.Function(this, 'copyToDestinationBucket', {
            functionName: `${cdk.Aws.STACK_NAME}-copyToDestinationBucket`,
            runtime: lambda.Runtime.NODEJS_14_X,
            handler: 'index.handler',
            memorySize: 128,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 100,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/copyToDestinationBucket')),
            environment:
            {
                DESTINATION_BUCKET: props.destinationBucket,
                STORAGE_CLASS: props.destinationStorageClass,
                STAGING_BUCKET: props.stagingBucket.bucketName,
                STAGING_BUCKET_PREFIX: 'stagingdata',
                STATUS_TABLE: props.statusTable.tableName
            }
        });

        props.stagingBucket.grantReadWrite(copyToDestinationBucket);
        props.statusTable.grantReadWriteData(copyToDestinationBucket);
        destinationBucket.grantReadWrite(copyToDestinationBucket);
        copyToDestinationBucket.addEventSource(new SqsEventSource(copyToDestinationBucketQueue, { batchSize: 1 }));
        CfnNagSuppressor.addLambdaSuppression(copyToDestinationBucket);
    }
}