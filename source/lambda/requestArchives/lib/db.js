// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();

const { STATUS_TABLE, METRICS_TABLE } = process.env;

async function getItem(pk) {
    return await dynamodb
        .getItem({
            TableName: METRICS_TABLE,
            Key: {
                pk: { S: pk },
            },
        })
        .promise();
}

async function decrementThrottledBytes(bytes, errorCount) {
    bytes = bytes * -1;
    errorCount = errorCount * -1;

    await dynamodb
        .updateItem({
            TableName: METRICS_TABLE,
            Key: {
                pk: {
                    S: "throttling",
                },
            },
            ExpressionAttributeValues: {
                ":throttledBytes": { N: `${bytes}` },
                ":errorCount": { N: `${errorCount}` },
            },
            ExpressionAttributeNames: {
                "#t": "throttledBytes",
                "#e": "errorCount",
            },
            UpdateExpression: "ADD #t :throttledBytes, #e :errorCount",
        })
        .promise();
}

async function getPartitionMaxProcessedFileNumber(pid) {
    console.log(`Checking last file number for partition : ${pid}`);
    let result = await dynamodb
        .query({
            TableName: STATUS_TABLE,
            IndexName: "max-file-index",
            KeyConditionExpression: "pid = :pid",
            ExpressionAttributeValues: {
                ":pid": { N: pid.toString() },
            },
            ProjectionExpression: "ifn, aid",
            ScanIndexForward: false,
            Limit: 1,
        })
        .promise();

    if (result.Count == 0) {
        console.log(`No records for partition ${pid} found. Setting the last item number to 0`);
        return 0;
    }

    const lastIfn = result.Items[0].ifn.N;
    const aid = result.Items[0].aid.S;

    console.log(`Last registered item is ${lastIfn}. ArchiveID (aid): ${aid} `);
    return lastIfn;
}

const filenameExists = async (fname) => {
    let result = await dynamodb
        .query({
            TableName: STATUS_TABLE,
            IndexName: "name-index",
            KeyConditionExpression: "fname = :fname",
            ExpressionAttributeValues: {
                ":fname": { S: fname },
            },
            Select: "COUNT",
            Limit: 1,
        })
        .promise();

    return result.Count !== 0;
};

module.exports = {
    getItem,
    decrementThrottledBytes,
    getPartitionMaxProcessedFileNumber,
    filenameExists,
};
