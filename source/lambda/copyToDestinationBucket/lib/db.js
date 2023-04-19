// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();

const moment = require("moment");

const { STATUS_TABLE } = process.env;

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
// staged - sgt
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

async function incrementRetryCount(archiveId, field) {
    let params = {
        TableName: STATUS_TABLE,
        Key: {
            aid: { S: archiveId },
        },
        UpdateExpression: "ADD #t :increment",
        ExpressionAttributeNames: {
            "#t": field,
        },
        ExpressionAttributeValues: { ":increment": { N: "1" } },
        ReturnValues: "ALL_NEW",
    };
    return await dynamodb.updateItem(params).promise();
}

module.exports = {
    setTimestampNow,
    updateChunkStatusGetLatest,
    getStatusRecord,
    incrementRetryCount,
};
