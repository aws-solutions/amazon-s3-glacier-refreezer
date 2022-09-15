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

'use strict';

import { Construct } from 'constructs';
import { RemovalPolicy, CfnResource, Duration, Aws } from 'aws-cdk-lib';
import { aws_dynamodb as dynamo } from 'aws-cdk-lib';   

const SCALING_IN_COOLDOWN_SEC = 60;
const SCALING_OUT_COOLDOWN_SEC = 10;
const TARGET_UTILIZATION = 70;

export class DynamoDataCatalog extends Construct {
    public readonly statusTable: dynamo.ITable;
    public readonly metricTable: dynamo.ITable;

    constructor(scope: Construct, id: string) {
        super(scope, id);

        // [ STATUS TABLE ]
        const statusTable = new dynamo.Table(this, "StatusTable", {
            tableName: `${Aws.STACK_NAME}-grf-job-status`,
            partitionKey: {name: "aid", type: dynamo.AttributeType.STRING},
            removalPolicy: RemovalPolicy.DESTROY,
            pointInTimeRecovery: true,
            billingMode: dynamo.BillingMode.PROVISIONED,
            readCapacity: 25,
            writeCapacity: 30,
            stream: dynamo.StreamViewType.NEW_AND_OLD_IMAGES
        });
        this.addCfnNagSuppressions(statusTable);
        // CDK excludes the default option - PROVISIONED - from the generated CFN.
        // Adding it explicitly to suppress automated review warning
        (<CfnResource>statusTable.node.defaultChild).addOverride('Properties.BillingMode', 'PROVISIONED');
        this.statusTable = statusTable;

        statusTable.autoScaleWriteCapacity({minCapacity: 30, maxCapacity: 500})
            .scaleOnUtilization({
                targetUtilizationPercent: TARGET_UTILIZATION,
                scaleOutCooldown: Duration.seconds(SCALING_OUT_COOLDOWN_SEC),
                scaleInCooldown: Duration.seconds(SCALING_IN_COOLDOWN_SEC)
            });

        statusTable
            .autoScaleReadCapacity({minCapacity: 25, maxCapacity: 500})
            .scaleOnUtilization({
                targetUtilizationPercent: TARGET_UTILIZATION,
                scaleOutCooldown: Duration.seconds(SCALING_OUT_COOLDOWN_SEC),
                scaleInCooldown: Duration.seconds(SCALING_IN_COOLDOWN_SEC)
            });

        // status table: max-file-index GSI
        statusTable.addGlobalSecondaryIndex({
            indexName: "max-file-index",
            partitionKey: {name: "pid", type: dynamo.AttributeType.NUMBER},
            sortKey: {name: "ifn", type: dynamo.AttributeType.NUMBER},
            projectionType: dynamo.ProjectionType.KEYS_ONLY,
            readCapacity: 5,
            writeCapacity: 30
        });
        DynamoDataCatalog.configureGsiAutoScaling(statusTable, 'max-file-index', 5, 30);

        // status table: file-name GSI
        statusTable.addGlobalSecondaryIndex({
            indexName: "name-index",
            partitionKey: {name: "fname", type: dynamo.AttributeType.STRING},
            projectionType: dynamo.ProjectionType.KEYS_ONLY,
            readCapacity: 15,
            writeCapacity: 30
        });
        DynamoDataCatalog.configureGsiAutoScaling(statusTable, 'name-index', 15, 30);

        // [ METRICS TABLE ]
        const metricsTable = new dynamo.Table(this, 'MetricsTable', {
            tableName: `${Aws.STACK_NAME}-grf-job-metrics`,
            partitionKey: {name: "pk", type: dynamo.AttributeType.STRING},
            removalPolicy: RemovalPolicy.DESTROY,
            pointInTimeRecovery: true,
            billingMode: dynamo.BillingMode.PAY_PER_REQUEST
        });
        this.addCfnNagSuppressions(metricsTable);
        this.metricTable = metricsTable;
    }

    static configureGsiAutoScaling(table: dynamo.Table, indexName: string, minReadCapacity: number, minWriteCapacity: number) {
        table.autoScaleGlobalSecondaryIndexReadCapacity(indexName, {
            minCapacity: minReadCapacity,
            maxCapacity: 500
        }).scaleOnUtilization({
            targetUtilizationPercent: TARGET_UTILIZATION,
            scaleOutCooldown: Duration.seconds(SCALING_OUT_COOLDOWN_SEC),
            scaleInCooldown: Duration.seconds(SCALING_IN_COOLDOWN_SEC)
        });

        table.autoScaleGlobalSecondaryIndexWriteCapacity(indexName, {
            minCapacity: minWriteCapacity,
            maxCapacity: 500
        }).scaleOnUtilization({
            targetUtilizationPercent: TARGET_UTILIZATION,
            scaleOutCooldown: Duration.seconds(SCALING_OUT_COOLDOWN_SEC),
            scaleInCooldown: Duration.seconds(SCALING_IN_COOLDOWN_SEC)
        });
    }

    private addCfnNagSuppressions(table: dynamo.ITable) {
        const rules = [{
            id: 'W28', reason: 'Transient table - updates must be through deletion/redeployment of the stack only'
        }, {
            id: 'W74', reason: 'Metadata table - no encryption required'
        }];

        const cfnTable = table.node.defaultChild as dynamo.CfnTable;
        cfnTable.cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress: rules
            }
        };
    }
}
