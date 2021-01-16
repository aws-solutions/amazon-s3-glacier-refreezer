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
    METRICS_TABLE,
} = process.env;

async function getCount() {
    try {
        const params = {
            KeyConditionExpression: 'pk = :pk',
            ExpressionAttributeValues: {
                ':pk': { S: 'count' }
            },
            TableName: METRICS_TABLE
        };
        const data = await dynamodb.query(params).promise();
        if (Array.isArray(data.Items) && data.Items.length) {
            return data.Items[0];
        } else {
            return null
        }
    } catch (error) {
        console.error('getCount.error', error);
    }
}

module.exports = { 
    getCount
};