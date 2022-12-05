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
import { CustomResource, Duration, Aws } from "aws-cdk-lib";
import { aws_lambda as lambda } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_sns as sns } from "aws-cdk-lib";
import { aws_stepfunctions as sfn } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import { aws_lambda_event_sources as eventsource } from "aws-cdk-lib";
import * as iamSec from "./iam-permissions";
import { CfnNagSuppressor } from "./cfn-nag-suppressor";
import * as path from "path";

export interface StageOneProps {
    readonly stagingBucket: s3.IBucket;
    readonly sourceGlacierVault: string;
    readonly destinationBucket: string;
    readonly destinationStorageClass: string;
    readonly glacierRetrievalTier: string;
    readonly filelistS3location: string;
    readonly cloudtrailExportConfirmation: string;
    readonly snsTopicForVaultConfirmation: string;
    readonly stageTwoOrchestrator: sfn.StateMachine;
}

export class StageOne extends Construct {
    constructor(scope: Construct, id: string, props: StageOneProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Inventory SNS Topic
        const inventoryTopic = new sns.Topic(this, "InventoryNotification");
        // overriding CDK name with CFN ID to enforce a random topic name generation
        // so if the same stack name has been deployed twice, each deployment will have only a single inventory alert
        (inventoryTopic.node.defaultChild as sns.CfnTopic).overrideLogicalId(`inventoryNotification`);
        inventoryTopic.addToResourcePolicy(iamSec.IamPermissions.snsDenyInsecureTransport(inventoryTopic));
        inventoryTopic.addToResourcePolicy(iamSec.IamPermissions.snsGlacierPublisher(inventoryTopic));
        CfnNagSuppressor.addSuppression(
            inventoryTopic,
            "W47",
            "Non sensitive metadata - encryption is not required and cost inefficient"
        );

        // -------------------------------------------------------------------------------------------
        // Request Inventory

        const requestInventoryRole = new iam.Role(this, "requestInventoryRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        const requestInventory = new lambda.Function(this, "requestInventory", {
            functionName: `${Aws.STACK_NAME}-requestInventory`,
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            memorySize: 256,
            timeout: Duration.minutes(15),
            code: lambda.Code.fromAsset("lambda/requestInventory"),
            role: requestInventoryRole,
            environment: {
                SOURCE_VAULT: props.sourceGlacierVault,
                STAGING_BUCKET: props.stagingBucket.bucketName,
                STAGING_LIST_PREFIX: "filelist",
                FILELIST_LOCATION: props.filelistS3location,
                DESTINATION_BUCKET: props.destinationBucket,
                SNS_TOPIC_ARN: inventoryTopic.topicArn,
                CLOUDTRAIL_EXPORT_CONF: props.cloudtrailExportConfirmation,
                SNS_VAULT_CONF: props.snsTopicForVaultConfirmation,
            },
        });

        const requestInventoryPolicy = new iam.Policy(this, "requestInventoryPolicy", {
            statements: [
                iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-requestInventory`),
                iamSec.IamPermissions.s3ReadOnly([`arn:${Aws.PARTITION}:s3:::${props.filelistS3location}`]),
                iamSec.IamPermissions.glacier(props.sourceGlacierVault),
            ],
        });
        requestInventoryPolicy.attachToRole(requestInventoryRole);
        props.stagingBucket.grantReadWrite(requestInventoryRole);
        s3.Bucket.fromBucketName(this, "destinationBucket", props.destinationBucket).grantReadWrite(
            requestInventoryRole
        );

        CfnNagSuppressor.addLambdaSuppression(requestInventory);

        const requestInventoryTrigger = new CustomResource(this, "requestInventoryTrigger", {
            serviceToken: requestInventory.functionArn,
        });

        // -------------------------------------------------------------------------------------------
        // Download Inventory Part
        const glacierAccess = iamSec.IamPermissions.glacier(props.sourceGlacierVault);

        const downloadInventoryPartRole = new iam.Role(this, "downloadInventoryPartRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        const downloadInventoryPart = new lambda.Function(this, "downloadInventoryPart", {
            functionName: `${Aws.STACK_NAME}-downloadInventoryPart`,
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            memorySize: 1024,
            reservedConcurrentExecutions: 10,
            timeout: Duration.minutes(15),
            code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/downloadInventoryPart")),
            role: downloadInventoryPartRole,
        });

        downloadInventoryPartRole.addToPrincipalPolicy(
            iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-downloadInventoryPart`)
        );
        props.stagingBucket.grantReadWrite(downloadInventoryPartRole);
        downloadInventoryPartRole.addToPrincipalPolicy(glacierAccess);
        CfnNagSuppressor.addLambdaSuppression(downloadInventoryPart);

        // -------------------------------------------------------------------------------------------
        // Download Inventory

        const downloadInventoryRole = new iam.Role(this, "downloadInventoryRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        const downloadInventory = new lambda.Function(this, "downloadInventory", {
            functionName: `${Aws.STACK_NAME}-downloadInventory`,
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            memorySize: 1024,
            timeout: Duration.minutes(15),
            code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/downloadInventory")),
            role: downloadInventoryRole,
            environment: {
                INVENTORY_BUCKET: props.stagingBucket.bucketName,
                BUCKET_PREFIX: "inventory",
                GLACIER_VAULT: props.sourceGlacierVault,
                STAGE_TWO_SF_ARN: props.stageTwoOrchestrator.stateMachineArn,
                INVENTORY_PART_FUNCTION: downloadInventoryPart.functionName,
            },
        });

        const downloadInventoryPolicy = new iam.Policy(this, "downloadInventoryPolicy", {
            statements: [iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-downloadInventory`), glacierAccess],
        });
        downloadInventoryPolicy.attachToRole(downloadInventoryRole);
        props.stagingBucket.grantReadWrite(downloadInventoryRole);
        downloadInventoryPart.grantInvoke(downloadInventoryRole);
        props.stageTwoOrchestrator.grant(downloadInventoryRole, "states:StartExecution");
        CfnNagSuppressor.addLambdaSuppression(downloadInventory);

        downloadInventory.addEventSource(new eventsource.SnsEventSource(inventoryTopic));
    }
}
