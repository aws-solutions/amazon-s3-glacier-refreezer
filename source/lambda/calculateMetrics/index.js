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
const dynamodb = new AWS.DynamoDB();

const {
    METRICS_TABLE
} = process.env;

async function handler(event) {

    let requested = 0
    let copyStarted = 0
    let copyCompleted = 0
    let validated = 0

    for (const record of event.Records) {
        if (record.eventName === "REMOVE") continue;
        requested += checkField(record, "aid")
        copyStarted += checkField(record, "psdt")
        copyCompleted += checkField(record, "cpdt")
        validated += checkField(record, "vdt")
    }

    if (requested > 0 || copyStarted > 0 || copyCompleted > 0 || validated > 0) {
        console.log(`r: ${requested} s: ${copyStarted} c: ${copyCompleted} v: ${validated}`)
        await incrementCount(requested, copyStarted, copyCompleted, validated)
    }
}

function checkField(record, field) {
    if ((!record.dynamodb.OldImage || !record.dynamodb.OldImage[field]) &&
        record.dynamodb.NewImage[field]) {
        return 1
    }
    return 0
}

async function incrementCount(requested, started, completed, validated) {
    await dynamodb.updateItem({
        TableName: METRICS_TABLE,
        Key: {
            pk: {
                S: "processProgress"
            }
        },
        ExpressionAttributeValues: {
            ":requested": { N: `${requested}` },
            ":started": { N: `${started}` },
            ":completed": { N: `${completed}` },
            ":validated": {
                N: `${validated}`
            }
        },
        UpdateExpression: "ADD requested :requested, started :started, completed :completed, validated :validated"
    }).promise();
}

module.exports = {
    handler,
    incrementCount
};
