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
import * as lambda from '@aws-cdk/aws-lambda';
import * as iamSec from './iam-permissions';
import * as path from 'path';
import * as dynamo from "@aws-cdk/aws-dynamodb";
import {SqsEventSource} from '@aws-cdk/aws-lambda-event-sources';
import {CfnNagSuppressor} from "./cfn-nag-suppressor";

export interface StageFourProps {
    readonly stagingBucket: s3.IBucket;
    readonly destinationBucket: string,
    readonly destinationStorageClass: string,
    readonly treehashCalcQueue: sqs.IQueue;
    readonly archiveNotificationQueue: sqs.IQueue;
    readonly statusTable: dynamo.ITable;
    readonly copyToDestinationBucketQueue: sqs.IQueue;
}

export class StageFour extends cdk.Construct {

    constructor(scope: cdk.Construct, id: string, props: StageFourProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Calculate Treehash and verify SHA256TreeHash Glacier == SHA256TreeHash S3
        const calculateTreehash = new lambda.Function(this, 'calculateTreehash', {
            functionName: `${cdk.Aws.STACK_NAME}-calculateTreehash`,
            runtime: lambda.Runtime.NODEJS_14_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 55,
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/calculateTreehash')),
            environment:
                {
                    DESTINATION_BUCKET: props.destinationBucket,
                    STORAGE_CLASS: props.destinationStorageClass,
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                    STAGING_BUCKET_PREFIX: 'stagingdata',
                    STATUS_TABLE: props.statusTable.tableName,
                    SQS_ARCHIVE_NOTIFICATION: props.archiveNotificationQueue.queueName,
                    SQS_COPY_TO_DESTINATION_NOTIFICATION: props.copyToDestinationBucketQueue.queueName
                }
        });

        props.stagingBucket.grantReadWrite(calculateTreehash);
        props.statusTable.grantReadWriteData(calculateTreehash);
        props.copyToDestinationBucketQueue.grantSendMessages(calculateTreehash);
        calculateTreehash.addEventSource(new SqsEventSource(props.treehashCalcQueue, {batchSize: 1}));
        CfnNagSuppressor.addLambdaSuppression(calculateTreehash);

        // -------------------------------------------------------------------------------------------
        // Create copyFinalToDestinationBucket function and copy archive from Staging to Destination
        const copyToDestinationBucket = new lambda.Function(this, 'copyToDestinationBucket', {
            functionName: `${cdk.Aws.STACK_NAME}-copyToDestinationBucket`,
            runtime: lambda.Runtime.NODEJS_14_X,
            handler: 'index.handler',
            memorySize: 128,
            timeout: cdk.Duration.minutes(15),
            reservedConcurrentExecutions: 55,
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
        s3.Bucket.fromBucketName(this, 'destinationBucket', props.destinationBucket).grantReadWrite(copyToDestinationBucket);
        copyToDestinationBucket.addEventSource(new SqsEventSource(props.copyToDestinationBucketQueue, {batchSize: 1}));
        CfnNagSuppressor.addLambdaSuppression(copyToDestinationBucket);
    }
}