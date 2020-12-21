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
import * as s3 from '@aws-cdk/aws-s3';
import * as iam from '@aws-cdk/aws-iam';

export class StagingBucket extends cdk.Construct {
    public readonly Bucket: s3.IBucket;

    constructor(scope: cdk.Construct, id: string) {
        super(scope, id);

        const bucketSettings: s3.BucketProps = {
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            encryption: s3.BucketEncryption.S3_MANAGED,
            removalPolicy: cdk.RemovalPolicy.DESTROY
        }

        this.Bucket = new s3.Bucket(this, 'StagingBucket', {
            ...bucketSettings
        });
        (this.Bucket.node.defaultChild as s3.CfnBucket).overrideLogicalId('stagingBucket');

        this.Bucket.addToResourcePolicy(
            new iam.PolicyStatement({
                resources: [
                    `${this.Bucket.bucketArn}`,
                    `${this.Bucket.bucketArn}/*`
                ],
                actions: ["s3:*"],
                principals: [new iam.AnyPrincipal],
                effect: iam.Effect.DENY,
                conditions: {
                    Bool: {
                        'aws:SecureTransport': 'false'
                    }
                }
            })
        );

        this.addCfnNagSuppressions(this.Bucket);
    }

    private addCfnNagSuppressions(bucket: s3.IBucket) {
        const rules = [{
            id: 'W35',
            reason: 'Transient bucket - access logs are not required'
        }]

        const cfnBucket = bucket.node.defaultChild as s3.CfnBucket;
        cfnBucket.cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress: rules
            }
        };
    }
}