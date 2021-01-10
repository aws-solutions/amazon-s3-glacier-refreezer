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
import * as sqs from '@aws-cdk/aws-sqs';
import * as sns from '@aws-cdk/aws-sns';
import * as subscriptions from '@aws-cdk/aws-sns-subscriptions';
import * as s3 from '@aws-cdk/aws-s3';
import * as lambda from '@aws-cdk/aws-lambda';
import * as iamSec from './iam-permissions';
import * as path from 'path';
import * as dynamo from "@aws-cdk/aws-dynamodb";
import {SqsEventSource} from '@aws-cdk/aws-lambda-event-sources';
import {CfnNagSuppressor} from "./cfn-nag-suppressor";
import * as iam from "@aws-cdk/aws-iam";

export interface StageThreeProps {
    readonly sourceVault: string;
    readonly stagingBucket: s3.IBucket;
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
        CfnNagSuppressor.addSuppression(treehashCalcQueue, 'W48', 'Non sensitive metadata - encryption is not required and cost inefficient');
        treehashCalcQueue.addToResourcePolicy(iamSec.IamPermissions.sqsDenyInsecureTransport(treehashCalcQueue));
        this.treehashCalcQueue = treehashCalcQueue;

        // -------------------------------------------------------------------------------------------
        // Archive Notification Topic
        const archiveNotificationTopic = new sns.Topic(this, 'archiveNotificationTopic', {
            topicName: `${cdk.Aws.STACK_NAME}-archive-retrieval-notification`,
        });
        CfnNagSuppressor.addSuppression(archiveNotificationTopic, 'W47', 'Non sensitive metadata - encryption is not required and cost inefficient');
        archiveNotificationTopic.addToResourcePolicy(iamSec.IamPermissions.snsGlacierPublisher(archiveNotificationTopic));
        archiveNotificationTopic.addToResourcePolicy(iamSec.IamPermissions.snsDenyInsecureTransport(archiveNotificationTopic));
        this.archiveNotificationTopic = archiveNotificationTopic;

        // -------------------------------------------------------------------------------------------
        // Archive Notification Queue
        const archiveNotificationQueue = new sqs.Queue(this, 'archive-notification-queue',
            {
                queueName: `${cdk.Aws.STACK_NAME}-archive-notification-queue`,
                visibilityTimeout: cdk.Duration.seconds(905)
            }
        );
        CfnNagSuppressor.addSuppression(archiveNotificationQueue, 'W48', 'Non sensitive metadata - encryption is not required and cost inefficient');
        archiveNotificationQueue.addToResourcePolicy(iamSec.IamPermissions.sqsDenyInsecureTransport(archiveNotificationQueue));
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
        CfnNagSuppressor.addSuppression(chunkCopyQueue, 'W48', 'Non sensitive metadata - encryption is not required and cost inefficient');
        chunkCopyQueue.addToResourcePolicy(iamSec.IamPermissions.sqsDenyInsecureTransport(chunkCopyQueue));

        // -------------------------------------------------------------------------------------------
        // Copy Archive
        const copyArchiveRole = new iam.Role(this, 'CopyArchiveRole', {
            assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
        });

        // Declaring the policy granting access to the stream explicitly to minimize permissions
        const copyArchiveRolePolicy = new iam.Policy(this, 'CopyArchiveRolePolicy', {
            statements: [
                iamSec.IamPermissions.lambdaLogGroup(`${cdk.Aws.STACK_NAME}-copyArchive`),
                iamSec.IamPermissions.glacier(props.sourceVault),
                iamSec.IamPermissions.sqsSubscriber(archiveNotificationQueue)
           ]
        });
        copyArchiveRolePolicy.attachToRole(copyArchiveRole);

        props.stagingBucket.grantReadWrite(copyArchiveRole);
        props.statusTable.grantReadWriteData(copyArchiveRole);
        chunkCopyQueue.grantSendMessages(copyArchiveRole);
        treehashCalcQueue.grantSendMessages(copyArchiveRole);
        // archiveNotificationQueue.grantConsumeMessages(copyArchiveRole);

        const copyArchive = new lambda.Function(this, 'CopyArchive', {
            functionName: `${cdk.Aws.STACK_NAME}-copyArchive`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 50,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/copyArchive')),
            role: copyArchiveRole.withoutPolicyUpdates(),
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
        copyArchive.node.addDependency(copyArchiveRolePolicy);
        copyArchive.addEventSource(new SqsEventSource(archiveNotificationQueue, {batchSize: 1}));
        CfnNagSuppressor.addW58Suppression(copyArchive);

        // -------------------------------------------------------------------------------------------
        // Copy Chunk
        const copyChunkRole = new iam.Role(this, 'CopyChunkRole', {
            assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
        });

        // Declaring the policy granting access to the stream explicitly to minimize permissions
        const copyChunkRolePolicy = new iam.Policy(this, 'CopyChunkRolePolicy', {
            statements: [
                iamSec.IamPermissions.lambdaLogGroup(`${cdk.Aws.STACK_NAME}-copyChunk`),
                iamSec.IamPermissions.glacier(props.sourceVault),
                iamSec.IamPermissions.sqsSubscriber(chunkCopyQueue)
            ]
        });
        copyChunkRolePolicy.attachToRole(copyChunkRole);

        props.stagingBucket.grantReadWrite(copyChunkRole);
        props.statusTable.grantReadWriteData(copyChunkRole);
        treehashCalcQueue.grantSendMessages(copyChunkRole);
        // archiveNotificationQueue.grantConsumeMessages(copyArchiveRole);

        const copyChunk = new lambda.Function(this, 'CopyChunk', {
            functionName: `${cdk.Aws.STACK_NAME}-copyChunk`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 30,
            role: copyChunkRole.withoutPolicyUpdates(),
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
        CfnNagSuppressor.addW58Suppression(copyChunk);
        copyChunk.addEventSource(new SqsEventSource(chunkCopyQueue, {batchSize: 1}));
    }
}
