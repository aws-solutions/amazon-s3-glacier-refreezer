// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const sqs = new AWS.SQS();

const db = require("./lib/db.js");

const { STAGING_BUCKET, STAGING_BUCKET_PREFIX, SQS_CHUNK } = process.env;

const CHUNK_SIZE = 4 * 1024 * 1024 * 1024;

async function handler(event) {
    // The call can be original (coming from galcier) or from the retry operation
    // Retry operation will NOT have all the fields.
    // Fields used:
    // ArchiveId
    // JobId
    // ArchiveSizeInBytes

    let snsMessage = JSON.parse(event.Records[0].body);
    let snsBody = snsMessage.Message;
    let glacierRetrievalStatus = JSON.parse(snsBody);

    let resultRecord = await db.getStatusRecord(glacierRetrievalStatus.ArchiveId);
    resultRecord.Attributes = resultRecord.Item;

    const key = resultRecord.Attributes.fname.S;

    if (resultRecord.Attributes.psdt && resultRecord.Attributes.psdt.S) {
        console.log(`${key} : processArchive has already started. Skipping`);
        return;
    }

    console.log(`${key} : copy started`);

    const numberOfChunks = parseInt(resultRecord.Attributes.cc.N);

    await processAndSendChunkMessage(glacierRetrievalStatus, key, numberOfChunks);
    await db.setTimestampNow(glacierRetrievalStatus.ArchiveId, "psdt"); // copy process started
}

async function processAndSendChunkMessage(glacierRetrievalStatus, key, numberOfChunks) {
    let queueUrl = await sqs.getQueueUrl({ QueueName: SQS_CHUNK }).promise();
    if (numberOfChunks > 1) {
        console.log(`${key} : multiPart : ${numberOfChunks}`);

        let multiPartUpload = await s3
            .createMultipartUpload({
                Bucket: STAGING_BUCKET,
                Key: `${STAGING_BUCKET_PREFIX}/${key}`,
            })
            .promise();

        let i = 1;
        while (i < numberOfChunks) {
            // console.log("Submitting chunk: " + i);
            let startByte = (i - 1) * CHUNK_SIZE;
            let endByte = startByte + CHUNK_SIZE - 1;

            await sendChunkMessage(
                queueUrl.QueueUrl,
                glacierRetrievalStatus,
                multiPartUpload.UploadId,
                key,
                i,
                startByte,
                endByte
            );
            i++;
        }

        // console.log("Processing last chunk: " + i);
        let startByte = (i - 1) * CHUNK_SIZE;
        let endByte = glacierRetrievalStatus.ArchiveSizeInBytes - 1;

        await sendChunkMessage(
            queueUrl.QueueUrl,
            glacierRetrievalStatus,
            multiPartUpload.UploadId,
            key,
            i,
            startByte,
            endByte
        );
    } else {
        await sendChunkMessage(queueUrl.QueueUrl, glacierRetrievalStatus, null, key, null, null, null);
    }
}

function sendChunkMessage(queueUrl, glacierRetrievalStatus, uploadId, key, partNo, startByte, endByte) {
    let messageBody = JSON.stringify({
        glacierRetrievalStatus,
        uploadId,
        key,
        partNo,
        startByte,
        endByte,
    });

    return sqs
        .sendMessage({
            QueueUrl: queueUrl,
            MessageBody: messageBody,
        })
        .promise();
}

module.exports = {
    handler,
};
