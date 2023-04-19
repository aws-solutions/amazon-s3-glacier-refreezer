// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const AWS = require("aws-sdk");
const dynamodb = new AWS.DynamoDB();

const { METRICS_TABLE } = process.env;

async function getItem(key) {
    try {
        const params = {
            KeyConditionExpression: "pk = :pk",
            ExpressionAttributeValues: {
                ":pk": { S: key },
            },
            TableName: METRICS_TABLE,
        };
        const data = await dynamodb.query(params).promise();
        if (Array.isArray(data.Items) && data.Items.length) {
            return data.Items[0];
        } else {
            return null;
        }
    } catch (error) {
        console.error("getItem.error", error);
    }
}

module.exports = {
    getItem,
};
