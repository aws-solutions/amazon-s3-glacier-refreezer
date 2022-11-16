/*********************************************************************************************************************
 *  Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
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

"use strict";

import { Construct } from "constructs";
import { CfnResource, Duration, Aws } from "aws-cdk-lib";
import { aws_dynamodb as dynamo } from "aws-cdk-lib";
import { aws_lambda as lambda } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_sqs as sqs } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_lambda_event_sources as eventsource } from "aws-cdk-lib";
import * as path from "path";
import * as iamSec from "./iam-permissions";
import { CfnNagSuppressor } from "./cfn-nag-suppressor";

export interface StageFourProps {
    readonly stagingBucket: s3.IBucket;
    readonly destinationBucket: string;
    readonly destinationStorageClass: string;
    readonly treehashCalcQueue: sqs.IQueue;
    readonly archiveNotificationQueue: sqs.IQueue;
    readonly statusTable: dynamo.ITable;
    readonly metricTable: dynamo.ITable;
}

export class StageFour extends Construct {
    constructor(scope: Construct, id: string, props: StageFourProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Get Destination S3 bucket
        const destinationBucket = s3.Bucket.fromBucketName(this, "destinationBucket", props.destinationBucket);

        // -------------------------------------------------------------------------------------------
        // copyToDestinationBucketQueue Queue
        const copyToDestinationBucketQueue = new sqs.Queue(this, "destination-copy-queue", {
            queueName: `${Aws.STACK_NAME}-destination-copy-queue`,
            retentionPeriod: Duration.days(14),
            visibilityTimeout: Duration.seconds(905),
        });
        CfnNagSuppressor.addSuppression(
            copyToDestinationBucketQueue,
            "W48",
            "Non sensitive metadata - encryption is not required and cost inefficient"
        );
        copyToDestinationBucketQueue.addToResourcePolicy(
            iamSec.IamPermissions.sqsDenyInsecureTransport(copyToDestinationBucketQueue)
        );

        // -------------------------------------------------------------------------------------------
        // Calculate Treehash and verify SHA256TreeHash Glacier == SHA256TreeHash S3
        const calculateTreehashRole = new iam.Role(this, "CalculateTreehashRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        const calculateTreehashRolePolicy = new iam.Policy(this, "CalculateTreehashRolePolicy", {
            statements: [
                iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-calculateTreehash`),
                new iam.PolicyStatement({
                    sid: "allowPutObject",
                    effect: iam.Effect.ALLOW,
                    actions: ["s3:PutObject"],
                    resources: [`${destinationBucket.bucketArn}/*`],
                }),
                new iam.PolicyStatement({
                    sid: "allowListBucket",
                    effect: iam.Effect.ALLOW,
                    actions: ["s3:ListBucket"],
                    resources: [`${destinationBucket.bucketArn}`],
                }),
            ],
        });
        calculateTreehashRolePolicy.attachToRole(calculateTreehashRole);

        props.stagingBucket.grantReadWrite(calculateTreehashRole);
        props.statusTable.grantReadWriteData(calculateTreehashRole);
        props.metricTable.grantReadWriteData(calculateTreehashRole);
        props.archiveNotificationQueue.grantSendMessages(calculateTreehashRole);
        copyToDestinationBucketQueue.grantSendMessages(calculateTreehashRole);

        const defaultCalculateTreehashPolicy = calculateTreehashRole.node.findChild("DefaultPolicy").node
            .defaultChild as CfnResource;
        CfnNagSuppressor.addCfnSuppression(defaultCalculateTreehashPolicy, "W76", "Policy is auto-generated by CDK");

        const calculateTreehash = new lambda.Function(this, "calculateTreehash", {
            functionName: `${Aws.STACK_NAME}-calculateTreehash`,
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            memorySize: 1024,
            timeout: Duration.minutes(15),
            reservedConcurrentExecutions: 55,
            code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/calculateTreehash")),
            role: calculateTreehashRole,
            environment: {
                DESTINATION_BUCKET: props.destinationBucket,
                STORAGE_CLASS: props.destinationStorageClass,
                STAGING_BUCKET: props.stagingBucket.bucketName,
                STAGING_BUCKET_PREFIX: "stagingdata",
                STATUS_TABLE: props.statusTable.tableName,
                METRIC_TABLE: props.metricTable.tableName,
                SQS_ARCHIVE_NOTIFICATION: props.archiveNotificationQueue.queueName,
                SQS_COPY_TO_DESTINATION_NOTIFICATION: copyToDestinationBucketQueue.queueName,
            },
        });
        calculateTreehash.addEventSource(new eventsource.SqsEventSource(props.treehashCalcQueue, { batchSize: 1 }));
        CfnNagSuppressor.addLambdaSuppression(calculateTreehash);

        // -------------------------------------------------------------------------------------------
        // Copy archive from Staging to Destination and delete Staging thereafter

        const copyToDestinationBucketRole = new iam.Role(this, "copyToDestinationBucketRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        const copyToDestinationBucket = new lambda.Function(this, "copyToDestinationBucket", {
            functionName: `${Aws.STACK_NAME}-copyToDestinationBucket`,
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            memorySize: 128,
            timeout: Duration.minutes(15),
            reservedConcurrentExecutions: 100,
            code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/copyToDestinationBucket")),
            role: copyToDestinationBucketRole,
            environment: {
                DESTINATION_BUCKET: props.destinationBucket,
                STORAGE_CLASS: props.destinationStorageClass,
                STAGING_BUCKET: props.stagingBucket.bucketName,
                STAGING_BUCKET_PREFIX: "stagingdata",
                STATUS_TABLE: props.statusTable.tableName,
            },
        });

        const copyToDestinationBucketPolicy = new iam.Policy(this, "copyToDestinationBucketPolicy", {
            statements: [iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-copyToDestinationBucket`)],
        });

        copyToDestinationBucketPolicy.attachToRole(copyToDestinationBucketRole);
        props.stagingBucket.grantReadWrite(copyToDestinationBucketRole);
        props.statusTable.grantReadWriteData(copyToDestinationBucketRole);
        destinationBucket.grantReadWrite(copyToDestinationBucketRole);
        copyToDestinationBucket.addEventSource(
            new eventsource.SqsEventSource(copyToDestinationBucketQueue, { batchSize: 1 })
        );
        CfnNagSuppressor.addLambdaSuppression(copyToDestinationBucket);
    }
}
