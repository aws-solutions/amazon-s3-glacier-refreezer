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

const AWS = require('aws-sdk');
const athena = new AWS.Athena();

const {
    DATABASE,
    STAGING_BUCKET,
    PARTITIONED_INVENTORY_TABLE,
    ATHENA_WORKGROUP
} = process.env;

async function handler() {
    console.log("Getting the min and max partitions from Athena")

    const queryExecution = await athena.startQueryExecution({
        QueryString:
            `SELECT MIN(part) as minPartition, MAX(part) as maxPartition FROM "${PARTITIONED_INVENTORY_TABLE}"`,
        QueryExecutionContext: {
            Database: DATABASE
        },
        ResultConfiguration: {
            OutputLocation: `s3://${STAGING_BUCKET}/results/`
        },
        WorkGroup: ATHENA_WORKGROUP
    }).promise()

    var runComplete = false;

    while (!runComplete) {
        console.log("Waiting for Athena Count Query to complete");
        await new Promise((resolve) => setTimeout(resolve, 5000));
        const result = await athena
            .getQueryExecution({QueryExecutionId: queryExecution.QueryExecutionId})
            .promise();
        if (!["QUEUED", "RUNNING", "SUCCEEDED"].includes(result.QueryExecution.Status.State)){
            console.error(`${JSON.stringify(result)}`)
            throw "Athena exception";
        }
        runComplete = result.QueryExecution.Status.State === "SUCCEEDED";
    }

    const result = await athena.getQueryResults({QueryExecutionId: queryExecution.QueryExecutionId}).promise();

    const minValue = parseInt(result.ResultSet.Rows[1].Data[0].VarCharValue);
    const maxValue = parseInt(result.ResultSet.Rows[1].Data[1].VarCharValue);

    console.info(`Min/Max Partitions : ${minValue} ${maxValue}`)

    if (isNaN(minValue) || isNaN(maxValue)){
        throw new Error('Failed to get partitions fromt the inventory table. Partitioning process failed?')
    }

    var retVal = {}

    retVal.currentPartition = minValue
    retVal.maxPartition = maxValue
    retVal.isComplete = false;

    return retVal;
}

module.exports = {
    handler
};
