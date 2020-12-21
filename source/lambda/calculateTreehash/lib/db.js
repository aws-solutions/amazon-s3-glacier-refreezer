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
// completed - cpdt
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

async function setRetryCount(archiveId, field) {
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
    setRetryCount,
};
