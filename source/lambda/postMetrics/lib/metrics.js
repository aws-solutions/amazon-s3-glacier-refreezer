/*********************************************************************************************************************
 *  Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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
const cloudwatch = new AWS.CloudWatch();

const {
    ARCHIVE_NOTIFICATIONS_TOPIC,
    STACK_NAME
} = process.env;

const CLOUDWATCH_DIMENSIONS_NAME = 'CloudFormationStack';
const CLOUDWATCH_NAMESPACE = 'AmazonS3GlacierReFreezer';

// publish a cloudwatch metric with a name and value
async function publishMetric(metricList) {
    // return if no metrics to be pushed.
    if (metricList.length == 0) return;

    let metricDataList = [];
    for (const metric of metricList) {
        metricDataList.push(
            {
                MetricName: metric.metricName,
                Dimensions: [{
                    Name: CLOUDWATCH_DIMENSIONS_NAME,
                    Value: STACK_NAME
                }],
                Unit: 'None',
                Value: metric.metricValue,
            }
        );
    }

    try {
        const params = {
            MetricData: metricDataList,
            Namespace: CLOUDWATCH_NAMESPACE
        };
        await cloudwatch.putMetricData(params).promise();
    } catch (error) {
        console.error('publishMetric.error', error);
        console.error('publishMetric.params', metricList);
    }
}

module.exports = { 
    publishMetric
};