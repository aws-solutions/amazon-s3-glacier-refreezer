/*********************************************************************************************************************
 *  Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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

"use strict";

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();

const { METRICS_TABLE } = process.env;

function checkField(record, field) {
    if ((!record.dynamodb.OldImage || !record.dynamodb.OldImage[field]) && record.dynamodb.NewImage[field]) {
        return 1;
    }
    return 0;
}

function getIncrementBytes(record, field) {
    if ((!record.dynamodb.OldImage || !record.dynamodb.OldImage[field]) && record.dynamodb.NewImage[field]) {
        return parseInt(record.dynamodb.NewImage["sz"]["N"]);
    }

    return 0;
}

async function incrementCount(requested, staged, validated, copied) {
    await dynamodb
        .updateItem({
            TableName: METRICS_TABLE,
            Key: {
                pk: {
                    S: "count",
                },
            },
            ExpressionAttributeValues: {
                ":requested": { N: `${requested}` },
                ":staged": { N: `${staged}` },
                ":validated": { N: `${validated}` },
                ":copied": { N: `${copied}` },
            },
            UpdateExpression: "ADD requested :requested, staged :staged, validated :validated, copied :copied",
        })
        .promise();
}

async function incrementBytes(requested, staged, validated, copied) {
    await dynamodb
        .updateItem({
            TableName: METRICS_TABLE,
            Key: {
                pk: {
                    S: "volume",
                },
            },
            ExpressionAttributeValues: {
                ":requested": { N: `${requested}` },
                ":staged": { N: `${staged}` },
                ":validated": { N: `${validated}` },
                ":copied": { N: `${copied}` },
            },
            UpdateExpression: "ADD requested :requested, staged :staged, validated :validated, copied :copied",
        })
        .promise();
}

module.exports = {
    checkField,
    getIncrementBytes,
    incrementCount,
    incrementBytes,
};
