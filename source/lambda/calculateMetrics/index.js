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

    let requested = 0;
    let staged = 0;
    let validated = 0;
    let copied = 0;

    for (const record of event.Records) {
        if (record.eventName === "REMOVE") continue;
        requested += dynamo.checkField(record, "aid");
        staged += dynamo.checkField(record, "sgt");
        validated += dynamo.checkField(record, "vdt");
        copied += dynamo.checkField(record, "cpt");
    }

    if (requested > 0 || staged > 0 || validated > 0 || copied > 0 ) {
        console.log(`r: ${requested} s: ${staged} v: ${validated} c: ${copied} `);
        await dynamo.incrementCount(requested, staged, validated, copied);
    }
}

module.exports = {
    handler
};
