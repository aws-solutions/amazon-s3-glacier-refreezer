/*********************************************************************************************************************
 *  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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
    handler
};
