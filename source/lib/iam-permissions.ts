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
import * as iam from '@aws-cdk/aws-iam';
import * as sns from '@aws-cdk/aws-sns';
import * as sqs from '@aws-cdk/aws-sqs';

export class IamPermissions extends cdk.Construct {

    constructor(scope: cdk.Construct, id: string) {
        super(scope, id);
    }

    static glacier(glacierVault: string) {
        return new iam.PolicyStatement({
            sid: 'allowGlacierAccess',
            effect: iam.Effect.ALLOW,
            actions: [
                'glacier:GetJobOutput',
                'glacier:InitiateJob'
            ],
            resources: [`arn:aws:glacier:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:vaults/${glacierVault}`],
            conditions: {
                Bool:
                    {'aws:SecureTransport': true}
            }
        });
    }


    static sqsSubscriber(queue: sqs.IQueue) {
        return new iam.PolicyStatement({
            sid: `allowSqsSubscribeAccess`,
            effect: iam.Effect.ALLOW,
            actions: [
                'sqs:ReceiveMessage',
                'sqs:DeleteMessage',
                'sqs:GetQueueAttributes'
            ],
            resources: [`${queue.queueArn}`]
        });
    }

    static snsDenyInsecureTransport(topic: sns.ITopic) {
        return new iam.PolicyStatement({
            sid: 'denyInsecureTransport',
            effect: iam.Effect.DENY,
            principals: [new iam.AnyPrincipal],
            actions: [
                'sns:GetTopicAttributes',
                'sns:SetTopicAttributes',
                'sns:AddPermission',
                'sns:RemovePermission',
                'sns:DeleteTopic',
                'sns:Subscribe',
                'sns:ListSubscriptionsByTopic',
                'sns:Publish'
            ],
            resources: [`${topic.topicArn}`],
            conditions: {
                Bool:
                    {'aws:SecureTransport': false}
            }
        });
    }

    static sqsDenyInsecureTransport(queue: sqs.IQueue) {
        return new iam.PolicyStatement({
            sid: 'denyInsecureTransport',
            effect: iam.Effect.DENY,
            principals: [new iam.AnyPrincipal],
            actions: [
                'sqs:*'
            ],
            resources: [`${queue.queueArn}`],
            conditions: {
                Bool:
                    {'aws:SecureTransport': false}
            }
        });
    }

    static snsGlacierPublisher(topic: sns.ITopic) {
        return new iam.PolicyStatement({
            sid: 'permitService',
            effect: iam.Effect.ALLOW,
            principals: [new iam.ServicePrincipal('glacier.amazonaws.com')],
            actions: [
                'sns:GetTopicAttributes',
                'sns:SetTopicAttributes',
                'sns:AddPermission',
                'sns:RemovePermission',
                'sns:DeleteTopic',
                'sns:Subscribe',
                'sns:ListSubscriptionsByTopic',
                'sns:Publish',
                'sns:Receive'
            ],
            resources: [`${topic.topicArn}`,],
            conditions: {
                "StringEquals": {
                    "AWS:SourceOwner": cdk.Aws.ACCOUNT_ID
                }
            }
        });
    }

    static s3ReadOnly(resources: Array<string>) {
        return new iam.PolicyStatement({
            sid: 'allowS3Access',
            effect: iam.Effect.ALLOW,
            actions: [
                's3:ListBucket',
                's3:ListBucketMultipartUploads',
                's3:GetBucketLocation',
                's3:GetObject',
                's3:GetObjectAcl'
            ],
            resources: resources,
            conditions: {
                Bool:
                    {'aws:SecureTransport': true}
            }
        })
    }

    static athena(resources: Array<string>) {
        return new iam.PolicyStatement({
            sid: 'allowStagingAccess',
            effect: iam.Effect.ALLOW,
            actions: [
                'athena:StartQueryExecution',
                'athena:GetQueryResults',
                'athena:GetWorkGroup',
                'athena:CancelQueryExecution',
                'athena:StopQueryExecution',
                'athena:GetQueryExecution',
                'glue:GetTable',
                'glue:UpdateTable',
                'glue:GetPartitions',
                'glue:BatchCreatePartition'
            ],
            resources: resources,
            conditions: {
                Bool:
                    {'aws:SecureTransport': true}
            }
        })
    }

    static lambdaLogGroup(functionName: string) {
        return new iam.PolicyStatement({
            sid: 'allowCloudWatchLogs',
            effect: iam.Effect.ALLOW,
            actions: [
                'logs:CreateLogGroup',
                'logs:CreateLogStream',
                'logs:PutLogEvents'
            ],
            resources: [
                `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws/lambda/${functionName}:**`
            ]
        })
    }
}
