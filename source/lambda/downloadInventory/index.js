/*********************************************************************************************************************
 *  Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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

const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const lambda = new AWS.Lambda();
const glacier = new AWS.Glacier();
const stepfunctions = new AWS.StepFunctions();

const { INVENTORY_BUCKET, BUCKET_PREFIX, GLACIER_VAULT, INVENTORY_PART_FUNCTION, STAGE_TWO_SF_ARN } = process.env;

const KEY = `${BUCKET_PREFIX}/${GLACIER_VAULT}-inventory.csv`;
const MAX_SIZE = 4 * 1024 * 1024 * 1024;

async function handler(event) {
    console.log(JSON.stringify(event));
    const request = JSON.parse(event.Records[0].Sns.Message);

    if (request.InventorySizeInBytes <= MAX_SIZE) {
        await inventorySinglePart(request.JobId, request.InventorySizeInBytes);
    } else {
        await inventoryMultiPart(request.JobId, request.InventorySizeInBytes);
    }

    await stepfunctions
        .startExecution({
            stateMachineArn: STAGE_TWO_SF_ARN,
        })
        .promise();

    console.log(`Done`);
}

async function inventorySinglePart(jobId, size) {
    console.log(`inventory - single part`);
    let inventoryStream = glacier
        .getJobOutput({
            accountId: "-",
            jobId: jobId,
            range: "",
            vaultName: GLACIER_VAULT,
        })
        .createReadStream();

    inventoryStream.length = size;

    return s3
        .putObject({
            Bucket: INVENTORY_BUCKET,
            Key: KEY,
            Body: inventoryStream,
        })
        .promise();
}

async function uploadPart(jobId, uploadId, partNo, startByte, endByte) {
    const request = {
        vault: GLACIER_VAULT,
        bucket: INVENTORY_BUCKET,
        key: KEY,
        jobId,
        uploadId,
        partNo,
        startByte,
        endByte,
    };

    const params = {
        FunctionName: INVENTORY_PART_FUNCTION,
        InvocationType: "RequestResponse",
        Payload: JSON.stringify(request),
    };

    return lambda.invoke(params).promise();
}

async function inventoryMultiPart(jobId, size) {
    console.log(`Multipart inventory download : ${GLACIER_VAULT}`);
    console.log(KEY);

    let multipartUpload = await s3
        .createMultipartUpload({
            Bucket: INVENTORY_BUCKET,
            Key: KEY,
        })
        .promise();

    const uploadId = multipartUpload.UploadId;
    console.log(`Multipart Upload ID : ${multipartUpload.UploadId}`);

    let partCopyRequests = [];
    let parts = Math.ceil(size / MAX_SIZE);
    var i = 1;

    console.log(`Inventory parts download`);
    while (i < parts) {
        const startByte = MAX_SIZE * (i - 1);
        const endByte = MAX_SIZE * i - 1;
        console.log(`Inventory part ${i} : ${startByte} - ${endByte} :`);
        partCopyRequests.push(uploadPart(jobId, uploadId, i, startByte, endByte));
        await sleep(10000);
        i++;
    }

    const startByte = MAX_SIZE * (i - 1);
    console.log(`Inventory part ${i} : ${startByte} - ${size - 1} :`);
    partCopyRequests.push(uploadPart(jobId, uploadId, i, startByte, size - 1));

    let data = await Promise.all(partCopyRequests);

    let eTags = [];
    try {
        data.forEach((item, index, array) => {
            let record = JSON.parse(item.Payload);
            eTags.push({ PartNumber: index + 1, ETag: record.ETag });
        });

        return s3
            .completeMultipartUpload({
                Bucket: INVENTORY_BUCKET,
                Key: KEY,
                MultipartUpload: { Parts: eTags },
                UploadId: uploadId,
            })
            .promise();
    } catch (e) {
        s3.abortMultipartUpload({
            Bucket: INVENTORY_BUCKET,
            Key: KEY,
            UploadId: uploadId,
        });
        throw e;
    }
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
    handler,
};
