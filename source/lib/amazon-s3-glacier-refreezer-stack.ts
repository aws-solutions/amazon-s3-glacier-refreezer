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
import {StagingBucket} from './s3-staging-bucket';
import {GlueDataCatalog} from './glue-data-catalog';
import {DynamoDataCatalog} from './ddb-data-catalog';
import {IamPermissions} from './iam-permissions';
import {StageOne} from './stage-one';
import {Monitoring} from "./monitoring";
import {StageThree} from "./stage-three";
import {StageFour} from "./stage-four";
import {SolutionStackProps} from './solution-props';
import {StageTwo} from "./stage-two";
import {AnonymousStatistics} from "./solution-builders-anonymous-statistics";
import * as sns from "@aws-cdk/aws-sns";
import {CfnNagSuppressor} from "./cfn-nag-suppressor";
import * as iamSec from "./iam-permissions";

export class AmazonS3GlacierRefreezerStack extends cdk.Stack {
    constructor(scope: cdk.Construct, id: string, props: SolutionStackProps) {
        super(scope, id, props);

        //---------------------------------------------------------------------
        // Amazon S3 Glacier CloudFormation Configuration
        const statisticsMapping = new cdk.CfnMapping(this, 'AnonymousStatisticsMap', {
            mapping: {
                'SendAnonymousStatistics': {
                    'Data': 'Yes'
                }
            }
        });

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
            allowedValues: ['STANDARD', 'INTELLIGENT_TIERING', 'STANDARD_IA', 'ONEZONE_IA', 'GLACIER_IR', 'GLACIER', 'DEEP_ARCHIVE']
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
        // Glue/Athena Data Structures
        const glueDataCatalog = new GlueDataCatalog(this, 'glueDataCatalog',
            {
                stagingBucket: stagingBucket.Bucket
            });

        //---------------------------------------------------------------------
        // DynamoDB Data Structures
        const dynamoDataCatalog = new DynamoDataCatalog(this, 'dynamoDataCatalog');

        //---------------------------------------------------------------------
        // IAM Permissions
        const iamPermissions = new IamPermissions(this, 'iamPermissions');

        // -------------------------------------------------------------------------------------------
        // Archive Notification Topic
        // Declaring at the stack level as it is shared between at Monitoring, Stage Two and  and Stage Three

        const archiveNotificationTopic = new sns.Topic(this, 'ArchiveNotifications');
        // overriding CDK name with CFN ID to enforce a random topic name generation
        // so even if the same stack name has been reused, each deployment will be isolated
        // Used as a safeguard for deployments and metric isolation
        (archiveNotificationTopic.node.defaultChild as sns.CfnTopic).overrideLogicalId(`archiveNotifications`);
        CfnNagSuppressor.addSuppression(archiveNotificationTopic, 'W47', 'Non sensitive metadata - encryption is not required and cost inefficient');
        archiveNotificationTopic.addToResourcePolicy(iamSec.IamPermissions.snsGlacierPublisher(archiveNotificationTopic));
        archiveNotificationTopic.addToResourcePolicy(iamSec.IamPermissions.snsDenyInsecureTransport(archiveNotificationTopic));

        //---------------------------------------------------------------------
        // Monitoring
        const monitoring = new Monitoring(this, `monitoring`, {
            statusTable: dynamoDataCatalog.statusTable,
            metricTable: dynamoDataCatalog.metricTable,
            archiveNotificationTopic
        })

        //---------------------------------------------------------------------
        // Anonymous Statistics

        // This solution includes an option to send anonymous operational metrics to
        // AWS. We use this data to better understand how customers use this
        // solution and related services and products

        const statistics = new AnonymousStatistics(this, `statistics`, {
            solutionId: props.solutionId,
            retrievalTier: glacierRetrievalTier.valueAsString,
            destinationStorageClass: destinationStorageClass.valueAsString,
            sendAnonymousSelection: statisticsMapping.findInMap('SendAnonymousStatistics', 'Data')
        })

        //---------------------------------------------------------------------
        // Stage Three: Copy Archives to Staging
        const stageThree = new StageThree(this, 'stageThree', {
            stagingBucket: stagingBucket.Bucket,
            sourceVault: sourceVault.valueAsString,
            statusTable: dynamoDataCatalog.statusTable,
            metricTable: dynamoDataCatalog.metricTable,
            archiveNotificationTopic
        });

        //---------------------------------------------------------------------
        // Stage Two: Request Archive Retrieval
        const stageTwo = new StageTwo(this, 'stageTwo', {
            stagingBucket: stagingBucket.Bucket,
            glacierSourceVault: sourceVault.valueAsString,
            glacierRetrievalTier: glacierRetrievalTier.valueAsString,
            glueDataCatalog: glueDataCatalog,
            dynamoDataCatalog: dynamoDataCatalog,
            sendAnonymousStats: statistics.sendAnonymousStats,
            archiveNotificationTopic
        });
        stageTwo.node.addDependency(monitoring);

        //---------------------------------------------------------------------
        // Stage One: Get Inventory
        const stageOne = new StageOne(this, 'stageOne', {
            stagingBucket: stagingBucket.Bucket,
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
            statusTable: dynamoDataCatalog.statusTable,
            treehashCalcQueue: stageThree.treehashCalcQueue,
            destinationBucket: destinationBucket.valueAsString,
            destinationStorageClass: destinationStorageClass.valueAsString,
            archiveNotificationQueue: stageThree.archiveNotificationQueue,
            metricTable: dynamoDataCatalog.metricTable
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

        new cdk.CfnOutput(this, 'dashboardUrl', {
            description: 'Progress Dashboard URL',
            value: `https://${cdk.Aws.REGION}.console.aws.amazon.com/cloudwatch/home?region=${cdk.Aws.REGION}#dashboards:name=${monitoring.dashboardName};accountId=${cdk.Aws.ACCOUNT_ID}`
        });
    }
}
