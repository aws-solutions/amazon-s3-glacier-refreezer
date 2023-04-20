// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

import { Construct } from "constructs";
import { Resource, CfnResource } from "aws-cdk-lib";

export class CfnNagSuppressor extends Construct {
    constructor(scope: Construct, id: string) {
        super(scope, id);
    }

    static addSuppression(resource: Resource, id: string, reason: string) {
        (<CfnResource>resource.node.defaultChild).cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress: [
                    {
                        id,
                        reason,
                    },
                ],
            },
        };
    }

    static addCfnSuppression(resource: CfnResource, id: string, reason: string) {
        resource.cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress: [
                    {
                        id,
                        reason,
                    },
                ],
            },
        };
    }

    static addLambdaSuppression(resource: Resource) {
        (<CfnResource>resource.node.defaultChild).cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress: [
                    {
                        id: "W58",
                        reason: "cfn nag is unable to reason about CDK generated cloudwatch log permissions",
                    },
                    {
                        id: "W89",
                        reason: "This is a fully serverless solution - no VPC is required",
                    },
                    {
                        id: "W92",
                        reason: "Reserved Concurrency is set on high priority functions only by design",
                    },
                ],
            },
        };
    }
}
