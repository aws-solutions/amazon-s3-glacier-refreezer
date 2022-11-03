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
