// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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
