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

export class CfnNagSuppressor extends cdk.Construct {

    constructor(scope: cdk.Construct, id: string) {
        super(scope, id);
    }

    static addSuppression(resource: cdk.Resource, id: string, reason: string) {
        (<cdk.CfnResource>resource.node.defaultChild).cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress:
                    [{
                        id,
                        reason
                    }]
            }
        };
    }
    static addSuppressions(resource: cdk.Resource, rules_to_suppress: Array<object>) {
        (<cdk.CfnResource>resource.node.defaultChild).cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress
            }
        };
    }

    static addW58Suppression(resource: cdk.Resource) {
        (<cdk.CfnResource>resource.node.defaultChild).cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress:
                    [{
                        id: 'W58',
                        reason: 'cfn nag is unable to reason about CDK generated cloudwatch log permissions'
                    }]
            }
        };
    }

}
