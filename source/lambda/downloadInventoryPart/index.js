// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const glacier = new AWS.Glacier();

async function handler(event) {
    console.log(`inventory - ${event.partNo}`);

    let inventoryStream = glacier
        .getJobOutput({
            accountId: "-",
            jobId: event.jobId,
            range: `bytes=${event.startByte}-${event.endByte}`,
            vaultName: event.vault,
        })
        .createReadStream();

    inventoryStream.length = event.endByte - event.startByte + 1;

    return await s3
        .uploadPart({
            UploadId: event.uploadId,
            Bucket: event.bucket,
            Key: event.key,
            PartNumber: event.partNo,
            Body: inventoryStream,
        })
        .promise();
}

module.exports = {
    handler,
};
