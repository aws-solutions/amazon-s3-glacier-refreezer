/*********************************************************************************************************************
 *  Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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

"use strict";

const moment = require("moment");
const axios = require("axios");

const { UUID, SOLUTION_ID, REGION, VERSION, STORAGE_CLASS, RETRIEVAL_TIER, SEND_ANONYMOUS_STATISTICS } = process.env;

const SOLUTION_BUILDERS_ENDPOINT = "https://metrics.awssolutionsbuilder.com/generic";

async function handler(event, context) {
    console.log(`${JSON.stringify(event)}`);
    let response;

    let anonymousData = {
        Solution: SOLUTION_ID,
        UUID: UUID,
        TimeStamp: moment().format(),
        Data: {
            Region: REGION,
            Version: VERSION.startsWith("%%") ? "0.9.0" : VERSION,
            StorageClass: STORAGE_CLASS,
            RetrievalTier: RETRIEVAL_TIER,
            VaultSize: event.vaultSize,
            ArchiveCount: event.archiveCount,
        },
    };
    console.log(anonymousData);

    if (SEND_ANONYMOUS_STATISTICS !== "Yes") {
        console.log("Sending anonymous data has been disabled. Exiting.");
        return;
    }

    let request = JSON.stringify(anonymousData);
    let params = {
        url: SOLUTION_BUILDERS_ENDPOINT,
        port: 443,
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "Content-Length": request.length,
        },
        data: request,
    };
    response = await axios(params);
    // console.log(response.data);
}

module.exports = {
    handler,
};
