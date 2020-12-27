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
import * as sns from '@aws-cdk/aws-sns';
import * as s3 from '@aws-cdk/aws-s3';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as lambda from '@aws-cdk/aws-lambda';
import {SnsEventSource} from '@aws-cdk/aws-lambda-event-sources';
import * as iamSec from './iam-security';
import * as path from 'path';

export interface StageOneProps {
    readonly stagingBucket: s3.IBucket;
    readonly logBucket: s3.IBucket;
    readonly iamSecurity: iamSec.IamSecurity;
    readonly sourceGlacierVault: string,
    readonly destinationBucket: string,
    readonly destinationStorageClass: string,
    readonly glacierRetrievalTier: string,
    readonly filelistS3location: string,
    readonly cloudtrailExportConfirmation: string,
    readonly snsTopicForVaultConfirmation: string
    readonly stageTwoOrchestrator: sfn.StateMachine
}

export class StageOne extends cdk.Construct {

    constructor(scope: cdk.Construct, id: string, props: StageOneProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Inventory SNS Topic
        const inventoryTopic = new sns.Topic(this, 'vaultInventoryTopic');
        // overriding CDK name with CFN ID to enforce a random topic name generation
        // so if the same stack name has been deployed twice, each deployment will have only a single inventory alert
        (inventoryTopic.node.defaultChild as sns.CfnTopic).overrideLogicalId(`vaultInventoryTopic`);
        inventoryTopic.addToResourcePolicy(iamSec.IamSecurity.snsDenyInsecureTransport(inventoryTopic));
        inventoryTopic.addToResourcePolicy(iamSec.IamSecurity.snsPermitAccountAccess(inventoryTopic));

        // -------------------------------------------------------------------------------------------
        // Request Inventory
        const requestInventory = new lambda.Function(this, 'requestInventory', {
            functionName: `${cdk.Aws.STACK_NAME}-requestInventory`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 256,
            timeout: cdk.Duration.minutes(15),
            code: lambda.Code.fromAsset('lambda/requestInventory'),
            environment:
                {
                    SOURCE_VAULT: props.sourceGlacierVault,
                    STAGING_BUCKET: props.stagingBucket.bucketName,
                    LOG_BUCKET: props.logBucket.bucketName,
                    STAGING_LIST_PREFIX: 'filelist',
                    FILELIST_LOCATION: props.filelistS3location,
                    DESTINATION_BUCKET: props.destinationBucket,
                    SNS_TOPIC_ARN: inventoryTopic.topicArn,
                    CLOUDTRAIL_EXPORT_CONF: props.cloudtrailExportConfirmation,
                    SNS_VAULT_CONF: props.snsTopicForVaultConfirmation
                }
        });

        props.stagingBucket.grantReadWrite(requestInventory);
        props.logBucket.grantReadWrite(requestInventory);
        s3.Bucket.fromBucketName(this, 'destinationBucket', props.destinationBucket).grantReadWrite(requestInventory);
        requestInventory.addToRolePolicy(iamSec.IamSecurity.s3AssureSecureTransport([`arn:aws:s3:::${props.filelistS3location}`]));
        requestInventory.addToRolePolicy(iamSec.IamSecurity.glacierPermitOperations(props.sourceGlacierVault));

        const requestInventoryTrigger = new cdk.CustomResource(this, 'requestInventoryTrigger',
            {
                serviceToken: requestInventory.functionArn
            });

        // -------------------------------------------------------------------------------------------
        // Download Inventory Part
        const glacierAccess = iamSec.IamSecurity.glacierPermitOperations(props.sourceGlacierVault);

        const downloadInventoryPart = new lambda.Function(this, 'downloadInventoryPart', {
            functionName: `${cdk.Aws.STACK_NAME}-downloadInventoryPart`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/downloadInventoryPart')),
        });

        props.stagingBucket.grantReadWrite(downloadInventoryPart);
        downloadInventoryPart.addToRolePolicy(glacierAccess);

        // -------------------------------------------------------------------------------------------
        // Download Inventory
        const downloadInventory = new lambda.Function(this, 'downloadInventory', {
            functionName: `${cdk.Aws.STACK_NAME}-downloadInventory`,
            runtime: lambda.Runtime.NODEJS_12_X,
            handler: 'index.handler',
            memorySize: 1024,
            timeout: cdk.Duration.minutes(15),
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/downloadInventory')),
            environment: {
                INVENTORY_BUCKET: props.stagingBucket.bucketName,
                BUCKET_PREFIX: 'inventory',
                GLACIER_VAULT: props.sourceGlacierVault,
                STAGE_TWO_SF_ARN: props.stageTwoOrchestrator.stateMachineArn,
                INVENTORY_PART_FUNCTION: downloadInventoryPart.functionName
            }
        });

        props.stagingBucket.grantReadWrite(downloadInventory);
        downloadInventory.addToRolePolicy(glacierAccess);
        downloadInventoryPart.grantInvoke(downloadInventory);
        props.stageTwoOrchestrator.grant(downloadInventory, 'states:StartExecution');

        downloadInventory.addEventSource(new SnsEventSource(inventoryTopic));
    }
}