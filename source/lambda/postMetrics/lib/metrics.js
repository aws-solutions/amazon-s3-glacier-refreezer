// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const AWS = require("aws-sdk");
const cloudwatch = new AWS.CloudWatch();

const { ARCHIVE_NOTIFICATIONS_TOPIC, STACK_NAME } = process.env;

const CLOUDWATCH_DIMENSIONS_NAME = "CloudFormationStack";
const CLOUDWATCH_NAMESPACE = "AmazonS3GlacierReFreezer";

// publish a cloudwatch metric with a name and value
async function publishMetric(metricList) {
    // return if no metrics to be pushed.
    if (metricList.length == 0) return;

    let metricDataList = [];
    for (const metric of metricList) {
        metricDataList.push({
            MetricName: metric.metricName,
            Dimensions: [
                {
                    Name: CLOUDWATCH_DIMENSIONS_NAME,
                    Value: STACK_NAME,
                },
            ],
            Unit: "None",
            Value: metric.metricValue,
        });
    }

    try {
        const params = {
            MetricData: metricDataList,
            Namespace: CLOUDWATCH_NAMESPACE,
        };
        await cloudwatch.putMetricData(params).promise();
    } catch (error) {
        console.error("publishMetric.error", error);
        console.error("publishMetric.params", metricList);
    }
}

module.exports = {
    publishMetric,
};
