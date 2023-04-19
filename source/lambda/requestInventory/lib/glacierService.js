// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const AWS = require("aws-sdk");
const glacier = new AWS.Glacier();

async function startJob(params) {
    const result = await glacier.initiateJob(params).promise();
    return result;
}

module.exports = {
    startJob,
};
