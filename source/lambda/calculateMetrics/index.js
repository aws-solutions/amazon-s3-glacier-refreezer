// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const dynamo = require("./lib/dynamo.js");

async function handler(event) {
    let requestedCount = 0;
    let stagedCount = 0;
    let validatedCount = 0;
    let copiedCount = 0;
    let requestedBytes = 0;
    let stagedBytes = 0;
    let validatedBytes = 0;
    let copiedBytes = 0;

    for (const record of event.Records) {
        if (record.eventName === "REMOVE") continue;
        requestedCount += dynamo.checkField(record, "cdt");
        stagedCount += dynamo.checkField(record, "sgt");
        validatedCount += dynamo.checkField(record, "vdt");
        copiedCount += dynamo.checkField(record, "cpt");

        requestedBytes += dynamo.getIncrementBytes(record, "cdt");
        stagedBytes += dynamo.getIncrementBytes(record, "sgt");
        validatedBytes += dynamo.getIncrementBytes(record, "vdt");
        copiedBytes += dynamo.getIncrementBytes(record, "cpt");
    }

    if (requestedCount > 0 || stagedCount > 0 || validatedCount > 0 || copiedCount > 0) {
        console.log(`r: ${requestedCount} s: ${stagedCount} v: ${validatedCount} c: ${copiedCount} `);
        await dynamo.incrementCount(requestedCount, stagedCount, validatedCount, copiedCount);

        console.log(`rb: ${requestedBytes} sb: ${stagedBytes} vb: ${validatedBytes} cb: ${copiedBytes} `);
        await dynamo.incrementBytes(requestedBytes, stagedBytes, validatedBytes, copiedBytes);
    }
}

module.exports = {
    handler,
};
