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
import * as cdk from '@aws-cdk/core';
import * as sqs from '@aws-cdk/aws-sqs';
import * as sns from '@aws-cdk/aws-sns';
import * as subscriptions from '@aws-cdk/aws-sns-subscriptions';
import * as s3 from '@aws-cdk/aws-s3';
import * as lambda from '@aws-cdk/aws-lambda';
import * as iamSec from './iam-security';
import * as path from 'path';
import * as dynamo from "@aws-cdk/aws-dynamodb";
import {SqsEventSource} from '@aws-cdk/aws-lambda-event-sources';

export interface StageThreeProps {
    readonly sourceVault: string;
    readonly stagingBucket: s3.IBucket;
    readonly iamSecurity: iamSec.IamSecurity;
    readonly statusTable: dynamo.ITable
}

export class StageThree extends cdk.Construct {

    readonly treehashCalcQueue: sqs.IQueue;
    readonly archiveNotificationTopic: sns.ITopic;
    readonly archiveNotificationQueue: sqs.IQueue;

    constructor(scope: cdk.Construct, id: string, props: StageThreeProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Treehash Calc Request Queue
        const treehashCalcQueue = new sqs.Queue(this, 'treehash-calc-queue',
            {
                queueName: `${cdk.Aws.STACK_NAME}-treehash-calc-queue`,
                visibilityTimeout: cdk.Duration.seconds(905)
            }
        );

        treehashCalcQueue.addToResourcePolicy(iamSec.IamSecurity.sqsDenyInsecureTransport(treehashCalcQueue));
        this.treehashCalcQueue = treehashCalcQueue;

        // -------------------------------------------------------------------------------------------
        // Archive Notification Topic
        const archiveNotificationTopic = new sns.Topic(this, 'archiveNotificationTopic');
        (archiveNotificationTopic.node.defaultChild as sns.CfnTopic).overrideLogicalId(`archiveNotificationTopic`);

        archiveNotificationTopic.addToResourcePolicy(iamSec.IamSecurity.snsPermitAccountAccess(archiveNotificationTopic));
        archiveNotificationTopic.addToResourcePolicy(iamSec.IamSecurity.snsDenyInsecureTransport(archiveNotificationTopic));
        this.archiveNotificationTopic = archiveNotificationTopic;

        // -------------------------------------------------------------------------------------------
        // Archive Notification Queue
        const archiveNotificationQueue = new sqs.Queue(this, 'archive-notification-queue',
            {
                queueName: `${cdk.Aws.STACK_NAME}-archive-notification-queue`,
                visibilityTimeout: cdk.Duration.seconds(905)
            }
        );

        archiveNotificationQueue.addToResourcePolicy(iamSec.IamSecurity.sqsDenyInsecureTransport(archiveNotificationQueue));
        archiveNotificationTopic.addSubscription(new subscriptions.SqsSubscription(archiveNotificationQueue));

        this.archiveNotificationQueue = archiveNotificationQueue;

        // -------------------------------------------------------------------------------------------
        // Chunk Copy Queue
        const chunkCopyQueue = new sqs.Queue(this, 'chunk-copy-queue',
            {
                queueName: `${cdk.Aws.STACK_NAME}-chunk-copy-queue`,
                visibilityTimeout: cdk.Duration.seconds(905)
            }
        );

        chunkCopyQueue.addToResourcePolicy(iamSec.IamSecurity.sqsDenyInsecureTransport(chunkCopyQueue));

        // -------------------------------------------------------------------------------------------
        // Copy Archive
        const copyArchive = new lambda.Function(this, 'copyArchive', {
            functionName: `${cdk.Aws.STACK_NAME}-copyArchive`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 50,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/copyArchive')),
            environment:
                {
                    VAULT: props.sourceVault,
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                    STAGING_BUCKET_PREFIX: 'stagingdata',
                    STATUS_TABLE: props.statusTable.tableName,
                    SQS_CHUNK: chunkCopyQueue.queueName,
                    SQS_HASH: treehashCalcQueue.queueName
                }
        });

        props.stagingBucket.grantReadWrite(copyArchive);
        props.statusTable.grantReadWriteData(copyArchive);
        copyArchive.addToRolePolicy(iamSec.IamSecurity.glacierPermitOperations(props.sourceVault));
        chunkCopyQueue.grantSendMessages(copyArchive);
        treehashCalcQueue.grantSendMessages(copyArchive);

        copyArchive.addEventSource(new SqsEventSource(archiveNotificationQueue,{ batchSize: 1 }));

        // -------------------------------------------------------------------------------------------
        // Copy Archive
        const copyChunk = new lambda.Function(this, 'copyChunk', {
            functionName: `${cdk.Aws.STACK_NAME}-copyChunk`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 30,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/copyChunk')),
            environment:
                {
                    VAULT: props.sourceVault,
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                    STAGING_BUCKET_PREFIX: 'stagingdata',
                    STATUS_TABLE: props.statusTable.tableName,
                    SQS_HASH: treehashCalcQueue.queueName
                }
        });

        props.stagingBucket.grantReadWrite(copyChunk);
        props.statusTable.grantReadWriteData(copyChunk);
        copyChunk.addToRolePolicy(iamSec.IamSecurity.glacierPermitOperations(props.sourceVault));
        treehashCalcQueue.grantSendMessages(copyChunk);

        copyChunk.addEventSource(new SqsEventSource(chunkCopyQueue,{ batchSize: 1 }));
    }
}