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
import * as iam from '@aws-cdk/aws-iam';
import * as sns from '@aws-cdk/aws-sns';
import * as sqs from '@aws-cdk/aws-sqs';

export class IamSecurity extends cdk.Construct {

    constructor(scope: cdk.Construct, id: string) {
        super(scope, id);
    }

    static glacierPermitOperations(glacierVault: string) {
        return new iam.PolicyStatement({
            sid: 'permitGlacier',
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

    static snsPermitAccountAccess(topic: sns.ITopic) {
        return new iam.PolicyStatement({
            sid: 'permitServie',
            effect: iam.Effect.ALLOW,
            principals: [new iam.AnyPrincipal],
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

    static s3AssureSecureTransport(resources: Array<string>) {
        return new iam.PolicyStatement({
            sid: 'allowStagingAccess',
            effect: iam.Effect.ALLOW,
            actions: [
                's3:ListBucket',
                's3:ListBucketVersions',
                's3:ListBucketMultipartUploads',
                's3:ListMultipartUploadParts',
                's3:GetBucketLocation',
                's3:GetObject',
                's3:GetObjectAcl',
                's3:PutObject',
                's3:DeleteObject',
                's3:AbortMultipartUpload'
            ],
            resources: resources,
            conditions: {
                Bool:
                    {'aws:SecureTransport': true}
            }
        })
    }

    static athenaPermissions(resources: Array<string>) {
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

}