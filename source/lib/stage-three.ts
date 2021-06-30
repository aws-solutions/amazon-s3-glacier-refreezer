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
    readonly archiveNotificationTopic: sns.ITopic
}

export class StageThree extends cdk.Construct {

    readonly treehashCalcQueue: sqs.IQueue;
    readonly archiveNotificationQueue: sqs.IQueue;

    constructor(scope: cdk.Construct, id: string, props: StageThreeProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Treehash Calc Request Queue
        const treehashCalcQueue = new sqs.Queue(this, 'treehash-calc-queue',
            {
                queueName: `${cdk.Aws.STACK_NAME}-treehash-calc-queue`,
                retentionPeriod: cdk.Duration.days(14),
                visibilityTimeout: cdk.Duration.seconds(905)
            }
        );
        CfnNagSuppressor.addSuppression(treehashCalcQueue, 'W48', 'Non sensitive metadata - encryption is not required and cost inefficient');
        treehashCalcQueue.addToResourcePolicy(iamSec.IamPermissions.sqsDenyInsecureTransport(treehashCalcQueue));
        this.treehashCalcQueue = treehashCalcQueue;

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
        props.archiveNotificationTopic.addSubscription(new subscriptions.SqsSubscription(archiveNotificationQueue));
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
        const processArchiveRole = new iam.Role(this, 'ProcessArchiveRole', {
            assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
        });

        // Declaring the policy granting access to the stream explicitly to minimize permissions
        const processArchiveRolePolicy = new iam.Policy(this, 'ProcessArchiveRolePolicy', {
            statements: [
                iamSec.IamPermissions.lambdaLogGroup(`${cdk.Aws.STACK_NAME}-processArchive`),
                iamSec.IamPermissions.glacier(props.sourceVault),
                iamSec.IamPermissions.sqsSubscriber(archiveNotificationQueue)
           ]
        });
        processArchiveRolePolicy.attachToRole(processArchiveRole);

        props.stagingBucket.grantReadWrite(processArchiveRole);
        props.statusTable.grantReadWriteData(processArchiveRole);
        chunkCopyQueue.grantSendMessages(processArchiveRole);
        treehashCalcQueue.grantSendMessages(processArchiveRole);

        const processArchive = new lambda.Function(this, 'ProcessArchive', {
            functionName: `${cdk.Aws.STACK_NAME}-processArchive`,
            runtime: lambda.Runtime.NODEJS_14_X,
            handler: 'index.handler',
            memorySize: 256,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 45,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/processArchive')),
            role: processArchiveRole.withoutPolicyUpdates(),
            environment:
                {
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                    STAGING_BUCKET_PREFIX: 'stagingdata',
                    STATUS_TABLE: props.statusTable.tableName,
                    SQS_CHUNK: chunkCopyQueue.queueName,
                    SQS_HASH: treehashCalcQueue.queueName
                }
        });
        processArchive.node.addDependency(processArchiveRolePolicy);
        processArchive.addEventSource(new SqsEventSource(archiveNotificationQueue, {batchSize: 1}));
        CfnNagSuppressor.addLambdaSuppression(processArchive);

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

        const copyChunk = new lambda.Function(this, 'CopyChunk', {
            functionName: `${cdk.Aws.STACK_NAME}-copyChunk`,
            runtime: lambda.Runtime.NODEJS_14_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 35,
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
        CfnNagSuppressor.addLambdaSuppression(copyChunk);
        copyChunk.addEventSource(new SqsEventSource(chunkCopyQueue, {batchSize: 1}));
    }
}
