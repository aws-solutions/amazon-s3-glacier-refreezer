// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();

const moment = require("moment");

const { STATUS_TABLE, METRIC_TABLE } = process.env;

async function getStatusRecord(archiveId) {
    return await dynamodb
        .getItem({
            TableName: STATUS_TABLE,
            Key: {
                aid: { S: archiveId },
            },
        })
        .promise();
}

// started   - psdt
// completed - sgt
async function setTimestampNow(archiveId, field) {
    const now = moment().format();
    return await dynamodb
        .updateItem({
            TableName: STATUS_TABLE,
            Key: {
                aid: { S: archiveId },
            },
            UpdateExpression: "set #t = :val",
            ExpressionAttributeNames: {
                "#t": field,
            },
            ExpressionAttributeValues: {
                ":val": { S: now },
            },
            ReturnValues: "ALL_NEW",
        })
        .promise();
}

async function increaseThrottleAndErrorCount(throttled, nBytes, value, nCount, count) {
    return await dynamodb
        .updateItem({
            TableName: METRIC_TABLE,
            Key: {
                pk: {
                    S: throttled,
                },
            },
            ExpressionAttributeNames: {
                "#t": nBytes,
                "#f": nCount,
            },
            ExpressionAttributeValues: {
                ":val": { N: value },
                ":count": { N: count },
            },
            UpdateExpression: "ADD #t :val, #f :count",
        })
        .promise();
}

async function updateChunkStatusGetLatest(archiveId, partNumber, val) {
    let params = {
        TableName: STATUS_TABLE,
        Key: {
            aid: { S: archiveId },
        },
        UpdateExpression: "set #f = :val",
        ExpressionAttributeNames: {
            "#f": `chunk${partNumber}`,
        },
        ExpressionAttributeValues: {
            ":val": { S: val },
        },
        ReturnValues: "ALL_NEW",
    };

    return await dynamodb.updateItem(params).promise();
}

module.exports = {
    setTimestampNow,
    updateChunkStatusGetLatest,
    getStatusRecord,
    increaseThrottleAndErrorCount,
};
