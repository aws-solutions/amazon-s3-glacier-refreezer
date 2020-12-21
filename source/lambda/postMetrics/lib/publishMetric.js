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
const cloudwatch = new AWS.CloudWatch();

const {
    METRICS_TABLE,
    STACK_NAME
} = process.env;

const CLOUDWATCH_DIMENSIONS_NAME = 'CloudFormation Stack';
const CLOUDWATCH_NAMESPACE = 'AmazonS3GlacierReFreezer';

// query dynamodb table for #pk = :totalRecordCount
async function getTotalRecords() {
    try {
        const params = {
            KeyConditionExpression: 'pk = :totalRecordCount',
            ExpressionAttributeValues: {
                ':totalRecordCount': { S: 'totalRecordCount' }
            },
            TableName: METRICS_TABLE
        };
        const data = await dynamodb.query(params).promise();
        if (Array.isArray(data.Items) && data.Items.length) {
            return parseInt(data.Items[0].value['N']);
        }
    } catch (error) {
        console.error('getTotalRecords.error', error);
    }
}

async function getProcessProgress() {
    try {
        const params = {
            KeyConditionExpression: 'pk = :pk',
            ExpressionAttributeValues: {
                ':pk': { S: 'processProgress' }
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
        console.error('getProcessProgress.error', error);
    }
}

// publish a cloudwatch metric with a name and value
async function publishMetric(metricName, metricValue) {
    // Not posting undefined value.
    if (metricValue==null) return;

    try {
        const params = {
            MetricData: [
                {
                    MetricName: metricName,
                    Dimensions: [{
                        Name: CLOUDWATCH_DIMENSIONS_NAME,
                        Value: STACK_NAME
                    }],
                    Unit: 'None',
                    Value: metricValue,
                },
            ],
            Namespace: CLOUDWATCH_NAMESPACE
        };
        const response = await cloudwatch.putMetricData(params).promise();
    } catch (error) {
        console.error('publishMetric.error', error);
        console.error('publishMetric.params', metricName, metricValue);
    }
}

module.exports = { 
    getTotalRecords, 
    getProcessProgress, 
    publishMetric 
};