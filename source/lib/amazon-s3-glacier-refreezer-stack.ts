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
import {StagingBucket} from './s3-staging-bucket';
import {GlueDataCatalog} from './glue-data-catalog';
import {DynamoDataCatalog} from './ddb-data-catalog';
import {IamSecurity} from './iam-security';
import {StageOne} from './stage-one';
import {Monitoring} from "./monitoring";
import {StageThree} from "./stage-three";
import {StageFour} from "./stage-four";
import {SolutionStackProps} from './solution-props';
import {StageTwo} from "./stage-two";
import {AnonymousStatistics} from "./solution-builders-anonymous-statistics";

export class AmazonS3GlacierRefreezerStack extends cdk.Stack {
    constructor(scope: cdk.Construct, id: string, props: SolutionStackProps) {
        super(scope, id, props);

        //---------------------------------------------------------------------
        // Amazon S3 Glacier CloudFormation Configuration
        const sourceVault = new cdk.CfnParameter(this, 'SourceVault', {
            type: 'String',
            allowedPattern: '.+'
        });

        const destinationBucket = new cdk.CfnParameter(this, 'DestinationBucket', {
            type: 'String',
            allowedPattern: '.+'
        });

        const destinationStorageClass = new cdk.CfnParameter(this, 'DestinationStorageClass', {
            type: 'String',
            default: 'STANDARD',
            allowedValues: ['STANDARD', 'INTELLIGENT_TIERING', 'STANDARD_IA', 'ONEZONE_IA', 'GLACIER', 'DEEP_ARCHIVE']
        });

        const glacierRetrievalTier = new cdk.CfnParameter(this, 'GlacierRetrievalTier', {
            type: 'String',
            default: 'Bulk',
            allowedValues: ['Bulk', 'Standard', 'Expedited']
        });

        const filelistS3location = new cdk.CfnParameter(this, 'FilelistS3Location', {
            type: 'String',
            default: ''
        });

        const cloudtrailExportConfirmation = new cdk.CfnParameter(this, 'CloudTrailExportConfirmation', {
            type: 'String',
            allowedValues: ['Yes', 'No']
        });

        const snsTopicForVaultConfirmation = new cdk.CfnParameter(this, 'SNSTopicForVaultConfirmation', {
            type: 'String',
            allowedValues: ['Yes', 'No']
        });

        //---------------------------------------------------------------------
        // Template metadata
        this.templateOptions.metadata = {
            'AWS::CloudFormation::Interface': {
                ParameterGroups: [
                    {
                        Label: {default: 'Required input parameters'},
                        Parameters: [sourceVault.logicalId, destinationBucket.logicalId, destinationStorageClass.logicalId, glacierRetrievalTier.logicalId]
                    },
                    {
                        Label: {default: 'Confirmation to avoid excessive costs'},
                        Parameters: [cloudtrailExportConfirmation.logicalId, snsTopicForVaultConfirmation.logicalId]
                    },
                    {
                        Label: {default: '[OPTIONAL] External filenames override for ArchiveDescription'},
                        Parameters: [filelistS3location.logicalId]
                    }
                ],
                ParameterLabels: {
                    [sourceVault.logicalId]: {
                        default: 'Source Glacier vault name'
                    },
                    [destinationBucket.logicalId]: {
                        default: 'S3 destination bucket name'
                    },
                    [destinationStorageClass.logicalId]: {
                        default: 'S3 destination storage class'
                    },

                    [glacierRetrievalTier.logicalId]: {
                        default: 'Glacier retrieval tier'
                    },
                    [filelistS3location.logicalId]: {
                        default: 'Amazon S3 location of the CSV file as BUCKET/FILEPATH'
                    },
                    [cloudtrailExportConfirmation.logicalId]: {
                        default: 'Have you checked that there is only one Cloudtrail export to S3 bucket configured in your account?'
                    },
                    [snsTopicForVaultConfirmation.logicalId]: {
                        default: 'Has default SNS notification topic on the vault been disabled or is it acceptable to receive notification for ALL archives in the vault?'
                    }
                }
            }
        };

        //---------------------------------------------------------------------
        // Staging S3 Bucket
        const stagingBucket = new StagingBucket(this, 'stagingBucket');

        //---------------------------------------------------------------------
        // IAM Security
        const iamSecurity = new IamSecurity(this, 'iamSecurity');

        //---------------------------------------------------------------------
        // DynamoDB Data Structures
        const dynamoDataCatalog = new DynamoDataCatalog(this, 'dynamoDataCatalog');

        //---------------------------------------------------------------------
        // Monitoring
        const monitoring = new Monitoring(this, `monitoring`, {
            statusTable: dynamoDataCatalog.statusTable,
            metricTable: dynamoDataCatalog.metricTable,
            iamSecurity: iamSecurity
        })

        //---------------------------------------------------------------------
        // Glue/Athena Data Structures
        const glueDataCatalog = new GlueDataCatalog(this, 'glueDataCatalog',
            {
                stagingBucket: stagingBucket.Bucket
            });

        //---------------------------------------------------------------------
        // Anonymous Statistics

        // This solution includes an option to send anonymous operational metrics to
        // AWS. We use this data to better understand how customers use this
        // solution and related services and products

        const statistics = new AnonymousStatistics(this, `statistics`, {
            solutionId: props.solutionId,
            retrievalTier: glacierRetrievalTier.valueAsString,
            destinationStorageClass: destinationStorageClass.valueAsString,
            sendAnonymousData: 'Yes'
        })

        //---------------------------------------------------------------------
        // Stage Three: Copy Archives to Staging
        const stageThree = new StageThree(this, 'stageThree', {
            stagingBucket: stagingBucket.Bucket,
            iamSecurity: iamSecurity,
            sourceVault: sourceVault.valueAsString,
            statusTable: dynamoDataCatalog.statusTable
        });

        //---------------------------------------------------------------------
        // Stage Two: Request Archive Retrieval
        const stageTwo = new StageTwo(this, 'stageTwo', {
            stagingBucket: stagingBucket.Bucket,
            iamSecurity: iamSecurity,
            glacierSourceVault: sourceVault.valueAsString,
            glacierRetrievalTier: glacierRetrievalTier.valueAsString,
            archiveNotificationTopic: stageThree.archiveNotificationTopic,
            glueDataCatalog: glueDataCatalog,
            dynamoDataCatalog: dynamoDataCatalog,
            sendAnonymousStats: statistics.sendAnonymousStats
        });
        stageTwo.node.addDependency(monitoring);

        //---------------------------------------------------------------------
        // Stage One: Get Inventory
        const stageOne = new StageOne(this, 'stageOne', {
            stagingBucket: stagingBucket.Bucket,
            logBucket: stagingBucket.LogBucket,
            iamSecurity: iamSecurity,
            sourceGlacierVault: sourceVault.valueAsString,
            destinationBucket: destinationBucket.valueAsString,
            destinationStorageClass: destinationStorageClass.valueAsString,
            glacierRetrievalTier: glacierRetrievalTier.valueAsString,
            filelistS3location: filelistS3location.valueAsString,
            cloudtrailExportConfirmation: cloudtrailExportConfirmation.valueAsString,
            snsTopicForVaultConfirmation: snsTopicForVaultConfirmation.valueAsString,
            stageTwoOrchestrator: stageTwo.stageTwoOrchestrator
        });
        stageOne.node.addDependency(monitoring);

        //---------------------------------------------------------------------
        // Stage Four: Validate SHA256 Treehash and move archives from Staging to Destination
        const stageFour = new StageFour(this, 'stageFour', {
            stagingBucket: stagingBucket.Bucket,
            iamSecurity: iamSecurity,
            statusTable: dynamoDataCatalog.statusTable,
            treehashCalcQueue: stageThree.treehashCalcQueue,
            destinationBucket: destinationBucket.valueAsString,
            destinationStorageClass: destinationStorageClass.valueAsString,
            archiveNotificationQueue: stageThree. archiveNotificationQueue
        });

        //---------------------------------------------------------------------
        // Tags
        cdk.Tags.of(this).add("solution", "amazon-s3-glacier-refreezer")

        //---------------------------------------------------------------------
        // Stack Outputs
        new cdk.CfnOutput(this, 'CloudTrailExportConfirmationSelection', {
            description: 'Selected option for CloudTrail Export confirmation',
            value: cloudtrailExportConfirmation.valueAsString
        });

        new cdk.CfnOutput(this, 'SNSTopicForVaultConfirmationSelection', {
            description: 'Selected option for SNS Topic for Vault confirmation',
            value: snsTopicForVaultConfirmation.valueAsString
        });

        new cdk.CfnOutput(this, 'StagingBucketName', {
            description: 'Staging Bucket Name',
            value: stagingBucket.Bucket.bucketName
        });

        new cdk.CfnOutput(this, 'StagingAccessLogBucketName', {
            description: 'Staging Access Logs Bucket Name',
            value: stagingBucket.LogBucket.bucketName
        });

        new cdk.CfnOutput(this, 'dashboardUrl', {
            description: 'Progress Dashboard URL',
            value: `https://${cdk.Aws.REGION}.console.aws.amazon.com/cloudwatch/home?region=${cdk.Aws.REGION}#dashboards:name=${monitoring.dashboardName};accountId=${cdk.Aws.ACCOUNT_ID}`
        });
    }
}
