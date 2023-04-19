// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

import { Construct } from "constructs";
import { CustomResource, Duration, Aws } from "aws-cdk-lib";
import { aws_lambda as lambda } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { CfnNagSuppressor } from "./cfn-nag-suppressor";
import * as iamSec from "./iam-permissions";

export interface AnonymousStatisticsProps {
    readonly solutionId: string;
    readonly retrievalTier: string;
    readonly destinationStorageClass: string;
    readonly sendAnonymousSelection: string;
}

export class AnonymousStatistics extends Construct {
    public sendAnonymousStats: lambda.IFunction;

    constructor(scope: Construct, id: string, props: AnonymousStatisticsProps) {
        super(scope, id);

        const generateUuidRole = new iam.Role(this, "generateUuidRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        const generateUuid = new lambda.Function(this, "GenerateUuid", {
            functionName: `${Aws.STACK_NAME}-generateUuid`,
            description: "This function generates UUID for each deployment",
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            memorySize: 256,
            timeout: Duration.seconds(20),
            code: lambda.Code.fromAsset("lambda/generateUuid"),
            role: generateUuidRole,
        });
        generateUuidRole.addToPrincipalPolicy(iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-generateUuid`));
        CfnNagSuppressor.addLambdaSuppression(generateUuid);

        const genereateUuidTrigger = new CustomResource(this, "GenerateUuidTrigger", {
            serviceToken: generateUuid.functionArn,
        });

        const sendAnonymousStatsRole = new iam.Role(this, "sendAnonymousStatsRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        const sendAnonymousStats = new lambda.Function(this, "SendAnonymousStats", {
            functionName: `${Aws.STACK_NAME}-sendAnonymousStats`,
            description: "This function sends anonymous statistics to the AWS Solutions Builders team",
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            memorySize: 128,
            timeout: Duration.minutes(5),
            code: lambda.Code.fromAsset("lambda/sendAnonymousStats"),
            role: sendAnonymousStatsRole,
            environment: {
                UUID: genereateUuidTrigger.getAttString("UUID"),
                REGION: Aws.REGION,
                SOLUTION_ID: props.solutionId,
                VERSION: "%%VERSION%%",
                STORAGE_CLASS: props.destinationStorageClass,
                RETRIEVAL_TIER: props.retrievalTier,
                SEND_ANONYMOUS_STATISTICS: props.sendAnonymousSelection,
            },
        });
        sendAnonymousStatsRole.addToPrincipalPolicy(
            iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-sendAnonymousStats`)
        );
        CfnNagSuppressor.addLambdaSuppression(sendAnonymousStats);
        this.sendAnonymousStats = sendAnonymousStats;
    }
}
