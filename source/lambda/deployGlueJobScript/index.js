// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const cloudformation = require("./lib/cloudformation");
const AWS = require("aws-sdk");
const s3 = new AWS.S3();
var fs = require("fs");

const { STAGING_BUCKET } = process.env;

const FILE_NAME = "partition-inventory.py";

async function handler(event, context) {
    console.log(JSON.stringify(event));

    //------------------------------------------------------------------------
    // [ ON CREATE ]
    if (event.RequestType === "Create") {
        console.log("Deploying Glue Job PySpark code");
        let readStream = fs.createReadStream(__dirname + "/" + FILE_NAME);
        let copyResult = await s3
            .putObject({
                Bucket: STAGING_BUCKET,
                Key: `glue/${FILE_NAME}`,
                Body: readStream,
            })
            .promise();

        console.log(JSON.stringify(copyResult));
        await cloudformation.sendResponse(event, context, "SUCCESS", { message: "Glue Script Copied" });
        return;
    }

    await cloudformation.sendResponse(event, context, "SUCCESS", {});
}

module.exports = {
    handler,
};
