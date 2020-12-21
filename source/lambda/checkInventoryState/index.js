/*********************************************************************************************************************
 *  Copyright 2019-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
 *                                                                                                                    *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    *
 *  with the License. A copy of the License is located at                                                             *
 *                                                                                                                    *
 *      http://www.apache.org/licenses/                                                                               *
 *                                                                                                                    *
 *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES *
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
 *  and limitations under the License.                                                                                *
 *********************************************************************************************************************/

/**
 * @author Solution Builders
 */

'use strict';

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();
const athena = new AWS.Athena();

const {
    METRICS_TABLE,
    DATABASE,
    INVENTORY_TABLE,
    PARTITIONED_TABLE,
    STAGING_BUCKET,
    ATHENA_WORKGROUP
} = process.env;

async function handler(payload) {

    console.log(`Checking inventory counts`);

    let inventoryCountQueryId = await startAthenaCountQuery(INVENTORY_TABLE);
    let partitionedCountQueryId = await startAthenaCountQuery(PARTITIONED_TABLE);

    let inventoryCount = await getAthenaCount(inventoryCountQueryId);
    let partitionedCount = await getAthenaCount(partitionedCountQueryId);

    console.log(`Inventory count: ${inventoryCount}`);
    console.log(`Partitioned count: ${partitionedCount}`);

    if (inventoryCount === 0){
        throw new Error('Inventory file does not exist or empty');
    }

    if (inventoryCount > 0 && inventoryCount === partitionedCount){
        console.log(`Partitioned table has been initialised, skipping`)
        return {skipInit: true}
    }

    // if (mergedCount > 0 && inventoryCount > 0 && partitionedCount === 0){
        // throw "Partitioned table is empty but filename merged table is not. Delete [STAGING_BUCKET]/merged and restart step functions orchestrator.";
    // }

    if (partitionedCount > 0 && inventoryCount > 0 && inventoryCount !== partitionedCount){
        throw 'Partitioned table is not empty but the record count differs form the inventory.';
    }

    await dynamodb
        .putItem({
            Item: AWS.DynamoDB.Converter.marshall({pk : 'totalRecordCount', value: inventoryCount}),
            TableName: METRICS_TABLE,
        })
        .promise();

    return {skipInit: false}
}

async function startAthenaCountQuery(tableName) {
    console.log(`Starting count for : ${tableName}`);

    const queryExecution = await athena
        .startQueryExecution({
            QueryString: `select count(*) from "${tableName}"`,
            QueryExecutionContext: {
                Database: DATABASE,
            },
            ResultConfiguration: {
                OutputLocation: `s3://${STAGING_BUCKET}/results/`,
            },
            WorkGroup: ATHENA_WORKGROUP
        })
        .promise();

    return queryExecution.QueryExecutionId;
}

async function getAthenaCount(executionId) {
    var runComplete = false;

    while (!runComplete) {
        console.log("Waiting for Athena Count Query to complete");
        await new Promise((resolve) => setTimeout(resolve, 5000));
        const result = await athena
            .getQueryExecution({QueryExecutionId: executionId})
            .promise();
        if (!["QUEUED", "RUNNING", "SUCCEEDED"].includes(result.QueryExecution.Status.State)){
            console.error(`${JSON.stringify(result)}`)
            throw "Athena exception";
        }
        runComplete = result.QueryExecution.Status.State === "SUCCEEDED";
    }

    const result = await athena.getQueryResults({QueryExecutionId: executionId}).promise();
    const count = parseInt(result.ResultSet.Rows[1].Data[0].VarCharValue);

    return count;
}

module.exports = {
    handler
};
