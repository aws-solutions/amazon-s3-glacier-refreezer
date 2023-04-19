// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

const axios = require("axios");

async function sendResponse(event, context, responseStatus, responseData) {
    let data;
    try {
        let responseBody = JSON.stringify({
            Status: responseStatus,
            Reason: "See the details in CloudWatch Log Stream: " + context.logGroupName + "/" + context.logStreamName,
            PhysicalResourceId: `${event.StackId}-${event.LogicalResourceId}`,
            StackId: event.StackId,
            RequestId: event.RequestId,
            LogicalResourceId: event.LogicalResourceId,
            Data: responseData,
        });
        let params = {
            url: event.ResponseURL,
            port: 443,
            method: "put",
            headers: {
                "content-type": "",
                "content-length": responseBody.length,
            },
            data: responseBody,
        };
        data = await axios(params);
    } catch (err) {
        throw err;
    }
    console.log(`Send response : ${data.status}`);
    return data.status;
}

module.exports = {
    sendResponse,
};
