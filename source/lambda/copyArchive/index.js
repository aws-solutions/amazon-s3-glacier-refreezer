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

const AWS = require("aws-sdk");
const glacier = new AWS.Glacier();
const s3 = new AWS.S3();
const sqs = new AWS.SQS();

const db = require("./lib/db.js");
const trigger = require("./lib/trigger.js");

const {
    VAULT,
    STAGING_BUCKET,
    STAGING_BUCKET_PREFIX,
    SQS_CHUNK
} = process.env;

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
    console.log("SNS Message body: " + snsBody);
    let glacierRetrievalStatus = JSON.parse(snsBody);

    // if sgt present, all chunks have been copied and multipart closed
    // but the message has been triggered, indicating retry. Proceed to trigger Treehash
    // if UplaodId exists - upload part
    let resultRecord = await db.getStatusRecord(glacierRetrievalStatus.ArchiveId);
    resultRecord.Attributes = resultRecord.Item;

    const key = resultRecord.Attributes.fname.S;
    console.log(`${key} : copy started`);

    //Remove this condition to check action flag.
    if (glacierRetrievalStatus.Action != "RetryRequest") {
        if (resultRecord.Attributes.sgt && resultRecord.Attributes.sgt.S) {
            console.log(`${key} : upload has already been processed`);
            if (!resultRecord.Attributes.vdt) {
                console.log(`${key} : re requesting treehash calc`);
                await trigger.calcHash(resultRecord);
            }
            return;
        }
    }

    console.log(`Setting start timestamp`);
    await db.setTimestampNow(glacierRetrievalStatus.ArchiveId, "psdt"); // copy process started

    const numberOfChunks = parseInt(resultRecord.Attributes.cc.N);

    if (numberOfChunks > 1) {
        await multiPart(glacierRetrievalStatus, key, numberOfChunks);
    } else {
        await singlePart(glacierRetrievalStatus, key);
    }
}

async function singlePart(glacierRetrievalStatus, key) {
    console.log(`${key} : single part`);
    let glacierStream = glacier
        .getJobOutput({
            accountId: "-",
            jobId: glacierRetrievalStatus.JobId,
            vaultName: VAULT,
        })
        .createReadStream();

    glacierStream.length = glacierRetrievalStatus.ArchiveSizeInBytes;

    let copyResult = await s3
        .putObject({
            Bucket: STAGING_BUCKET,
            Key: `${STAGING_BUCKET_PREFIX}/${key}`,
            Body: glacierStream,
        })
        .promise();

    let etag = copyResult.ETag;

    console.log(`${key} : etag : ${etag}`);
    let statusRecord = await db.setTimestampNow(
        glacierRetrievalStatus.ArchiveId,
        "sgt"
    );
    await trigger.calcHash(statusRecord);
}

async function multiPart(glacierRetrievalStatus, key, numberOfChunks) {
    console.log(`${key} : multiPart : ${numberOfChunks}`);

    let queueUrl = await sqs.getQueueUrl({ QueueName: SQS_CHUNK }).promise();
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
            glacierRetrievalStatus.JobId,
            multiPartUpload.UploadId,
            glacierRetrievalStatus.ArchiveId,
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
            glacierRetrievalStatus.JobId,
            multiPartUpload.UploadId,
            glacierRetrievalStatus.ArchiveId,
            key,
            i,
            startByte,
            endByte
        );
}

function sendChunkMessage(queueUrl, jobId, uploadId, archiveId, key, partNo, startByte, endByte) {
    let messageBody = JSON.stringify({
        jobId,
        uploadId,
        archiveId,
        key,
        partNo,
        startByte,
        endByte,
    });

    return sqs.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: messageBody,
        }).promise();
}

module.exports = {
    handler
};
