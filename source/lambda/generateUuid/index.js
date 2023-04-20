// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const cloudformation = require("./lib/cloudformation");
const uuid = require("uuid");

async function handler(event, context) {
    console.log(`${JSON.stringify(event)}`);

    //------------------------------------------------------------------------
    // [ ON CREATE ]
    if (event.RequestType === "Create") {
        console.log("Generating deployment UUID");
        const uuidv4 = uuid.v4();

        let responseData = {
            UUID: uuidv4,
        };
        console.log(responseData.UUID);
        await cloudformation.sendResponse(event, context, "SUCCESS", responseData);
        return;
    }

    let responseData = { message: "OK" };
    await cloudformation.sendResponse(event, context, "SUCCESS", responseData);
}

module.exports = {
    handler,
};
