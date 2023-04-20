// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const cloudformation = require("./lib/cloudformation");

exports.handler = function (event, context) {
    console.log(JSON.stringify(event));
    let responseData = {};
    for (const [key, val] of Object.entries(event.ResourceProperties)) {
        responseData[key] = val.toLowerCase();
    }
    console.log(responseData);
    try {
        cloudformation.sendResponse(event, context, "SUCCESS", responseData);
    } catch (err) {
        console.error(err);
        cloudformation.sendResponse(event, context, "FAILED", { message: `${JSON.stringify(err)}` });
    }
};
