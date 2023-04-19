// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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
