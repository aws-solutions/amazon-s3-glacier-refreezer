// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

import { Construct } from "constructs";
import { Aws } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_sqs as sqs } from "aws-cdk-lib";
import { aws_sns as sns } from "aws-cdk-lib";

export class IamPermissions extends Construct {
    constructor(scope: Construct, id: string) {
        super(scope, id);
    }

    static glacier(glacierVault: string) {
        return new iam.PolicyStatement({
            sid: "allowGlacierAccess",
            effect: iam.Effect.ALLOW,
            actions: ["glacier:GetJobOutput", "glacier:InitiateJob"],
            resources: [`arn:${Aws.PARTITION}:glacier:${Aws.REGION}:${Aws.ACCOUNT_ID}:vaults/${glacierVault}`],
            conditions: {
                Bool: { "aws:SecureTransport": true },
            },
        });
    }

    static sqsSubscriber(queue: sqs.IQueue) {
        return new iam.PolicyStatement({
            sid: `allowSqsSubscribeAccess`,
            effect: iam.Effect.ALLOW,
            actions: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
            resources: [`${queue.queueArn}`],
        });
    }

    static snsDenyInsecureTransport(topic: sns.ITopic) {
        return new iam.PolicyStatement({
            sid: "denyInsecureTransport",
            effect: iam.Effect.DENY,
            principals: [new iam.AnyPrincipal()],
            actions: [
                "sns:GetTopicAttributes",
                "sns:SetTopicAttributes",
                "sns:AddPermission",
                "sns:RemovePermission",
                "sns:DeleteTopic",
                "sns:Subscribe",
                "sns:ListSubscriptionsByTopic",
                "sns:Publish",
            ],
            resources: [`${topic.topicArn}`],
            conditions: {
                Bool: { "aws:SecureTransport": false },
            },
        });
    }

    static sqsDenyInsecureTransport(queue: sqs.IQueue) {
        return new iam.PolicyStatement({
            sid: "denyInsecureTransport",
            effect: iam.Effect.DENY,
            principals: [new iam.AnyPrincipal()],
            actions: ["sqs:*"],
            resources: [`${queue.queueArn}`],
            conditions: {
                Bool: { "aws:SecureTransport": false },
            },
        });
    }

    static snsGlacierPublisher(topic: sns.ITopic) {
        return new iam.PolicyStatement({
            sid: "permitService",
            effect: iam.Effect.ALLOW,
            principals: [new iam.ServicePrincipal("glacier.amazonaws.com")],
            actions: [
                "sns:GetTopicAttributes",
                "sns:SetTopicAttributes",
                "sns:AddPermission",
                "sns:RemovePermission",
                "sns:DeleteTopic",
                "sns:Subscribe",
                "sns:ListSubscriptionsByTopic",
                "sns:Publish",
                "sns:Receive",
            ],
            resources: [`${topic.topicArn}`],
            conditions: {
                StringEquals: {
                    "AWS:SourceOwner": Aws.ACCOUNT_ID,
                },
            },
        });
    }

    static s3ReadOnly(resources: Array<string>) {
        return new iam.PolicyStatement({
            sid: "allowS3Access",
            effect: iam.Effect.ALLOW,
            actions: [
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:GetObjectAcl",
            ],
            resources: resources,
            conditions: {
                Bool: { "aws:SecureTransport": true },
            },
        });
    }

    static athena(resources: Array<string>) {
        return new iam.PolicyStatement({
            sid: "allowStagingAccess",
            effect: iam.Effect.ALLOW,
            actions: [
                "athena:StartQueryExecution",
                "athena:GetQueryResults",
                "athena:GetWorkGroup",
                "athena:CancelQueryExecution",
                "athena:StopQueryExecution",
                "athena:GetQueryExecution",
                "glue:GetTable",
                "glue:UpdateTable",
                "glue:GetPartitions",
                "glue:BatchCreatePartition",
            ],
            resources: resources,
            conditions: {
                Bool: { "aws:SecureTransport": true },
            },
        });
    }

    static lambdaLogGroup(functionName: string) {
        return new iam.PolicyStatement({
            sid: "allowCloudWatchLogs",
            effect: iam.Effect.ALLOW,
            actions: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            resources: [
                `arn:${Aws.PARTITION}:logs:${Aws.REGION}:${Aws.ACCOUNT_ID}:log-group:/aws/lambda/${functionName}:**`,
            ],
        });
    }
}
