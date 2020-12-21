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
import * as dynamo from '@aws-cdk/aws-dynamodb';

const SCALING_IN_COOLDOWN_SEC = 60;
const SCALING_OUT_COOLDOWN_SEC = 10;
const TARGET_UTILIZATION = 70;

export class DynamoDataCatalog extends cdk.Construct {
    public readonly statusTable: dynamo.ITable;
    public readonly metricTable: dynamo.ITable;

    constructor(scope: cdk.Construct, id: string) {
        super(scope, id);

        // [ STATUS TABLE ]
        const statusTable = new dynamo.Table(this, "statusTable", {
            tableName: `${cdk.Aws.STACK_NAME}-grf-job-status`,
            partitionKey: {name: "aid", type: dynamo.AttributeType.STRING},
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            billingMode: dynamo.BillingMode.PROVISIONED,
            readCapacity: 25,
            writeCapacity: 30,
            stream: dynamo.StreamViewType.NEW_AND_OLD_IMAGES
        });
        this.statusTable=statusTable;

        statusTable.autoScaleWriteCapacity({minCapacity: 30, maxCapacity: 500})
            .scaleOnUtilization({
                targetUtilizationPercent: TARGET_UTILIZATION,
                scaleOutCooldown: cdk.Duration.seconds(SCALING_OUT_COOLDOWN_SEC),
                scaleInCooldown: cdk.Duration.seconds(SCALING_IN_COOLDOWN_SEC)
            });

        statusTable
            .autoScaleReadCapacity({minCapacity: 25, maxCapacity: 500})
            .scaleOnUtilization({
                targetUtilizationPercent: TARGET_UTILIZATION,
                scaleOutCooldown: cdk.Duration.seconds(SCALING_OUT_COOLDOWN_SEC),
                scaleInCooldown: cdk.Duration.seconds(SCALING_IN_COOLDOWN_SEC)
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
        DynamoDataCatalog.configureGsiAutoScaling(statusTable,'max-file-index', 5, 30 );

        // status table: file-name GSI
        statusTable.addGlobalSecondaryIndex({
            indexName: "name-index",
            partitionKey: {name: "fname", type: dynamo.AttributeType.STRING},
            projectionType: dynamo.ProjectionType.KEYS_ONLY,
            readCapacity: 15,
            writeCapacity: 30
        });
        DynamoDataCatalog.configureGsiAutoScaling(statusTable,'name-index', 15, 30 );

        // [ METRICS TABLE ]
        const metricsTable = new dynamo.Table(this, 'metricsTable', {
            tableName: `${cdk.Aws.STACK_NAME}-grf-job-metrics`,
            partitionKey: {name: "pk", type: dynamo.AttributeType.STRING},
            removalPolicy: cdk.RemovalPolicy.DESTROY,
            billingMode: dynamo.BillingMode.PAY_PER_REQUEST
        });
        this.metricTable = metricsTable;
    }

    static configureGsiAutoScaling(table: dynamo.Table, indexName: string, minReadCapacity: number, minWriteCapacity: number) {
        table.autoScaleGlobalSecondaryIndexReadCapacity(indexName, {
            minCapacity: minReadCapacity,
            maxCapacity: 500
        }).scaleOnUtilization({
            targetUtilizationPercent: TARGET_UTILIZATION,
            scaleOutCooldown: cdk.Duration.seconds(SCALING_OUT_COOLDOWN_SEC),
            scaleInCooldown: cdk.Duration.seconds(SCALING_IN_COOLDOWN_SEC)
        });

        table.autoScaleGlobalSecondaryIndexWriteCapacity(indexName, {
            minCapacity: minWriteCapacity,
            maxCapacity: 500
        }).scaleOnUtilization({
            targetUtilizationPercent: TARGET_UTILIZATION,
            scaleOutCooldown: cdk.Duration.seconds(SCALING_OUT_COOLDOWN_SEC),
            scaleInCooldown: cdk.Duration.seconds(SCALING_IN_COOLDOWN_SEC)
        });
    }
}
