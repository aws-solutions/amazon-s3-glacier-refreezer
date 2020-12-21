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
    incrementCount
}