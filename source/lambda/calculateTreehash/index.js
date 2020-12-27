/*********************************************************************************************************************
 *  Copyright 2019-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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
const s3 = new AWS.S3();
const sqs = new AWS.SQS();

const db = require("./lib/db.js");
const treehash = require("./lib/treehash.js");
const copy = require("./lib/copy.js");

const {
    DESTINATION_BUCKET,
    STAGING_BUCKET,
    STAGING_BUCKET_PREFIX,
    SQS_ARCHIVE_NOTIFICATION,
} = process.env;

async function handler(event) {
    let {aid, key, partNo, startByte, endByte} = JSON.parse(event.Records[0].body);

    console.log(`${key} - ${partNo} hash : ${startByte}-${endByte}`);

    let resultRecord = await db.getStatusRecord(aid);
    if (resultRecord.Item.vdt && resultRecord.Item.vdt.S) {
        console.log(`${key} : treehash has already been processed. Skipping`);
        return;
    }

    const file = await fileExists(DESTINATION_BUCKET, key);
    if (file) {
        console.error(`${key} : already exists in the target bucket. Not overwriting: ${file.StorageClass}`);
        await db.setTimestampNow(aid, "vdt");
        return;
    }

    let cc = parseInt(resultRecord.Item.cc.N);

    let keyHash = await treehash.getChunkHash(key, partNo, startByte, endByte);

    // Single Part
    if (cc == 1) {
        resultRecord.Attributes = resultRecord.Item;
        await finalise(keyHash, resultRecord);
        return;
    }

    // Multi Part
    let statusRecord = await db.updateChunkStatusGetLatest(aid, partNo, keyHash);

    let count = 0;
    for (const entry in statusRecord.Attributes) {
        if (
            entry.includes("chunk") &&
            statusRecord.Attributes[entry].S &&
            statusRecord.Attributes[entry].S.length > 40 // to validate that the field contains hash as opposed to etag
        ) {
            count++;
        }
    }

    if (count < cc) return; // not all chunks have yet been completed

    let s3hash = treehash.calculateMultiPartHash(statusRecord);
    await finalise(s3hash, statusRecord);
};

async function finalise(s3hash, statusRecord) {
    let key = statusRecord.Attributes.fname.S;
    let glacierHash = statusRecord.Attributes.sha.S;

    if (s3hash != glacierHash) {
        console.error(`ERROR : sha256treehash validation failed : ${key}.\n`);
        console.log(`SHA256TreeHash Glacier : ${glacierHash}.`);
        console.log(`SHA256TreeHash S3      : ${s3hash}.`);

        let retryCount = parseInt(statusRecord.Attributes.rc.N);
        if (retryCount < 3) {
            await failArchiveAndRetry(statusRecord, key);
            return;
        } else {
            console.error(
                `ERROR : sha256treehash validation failed after MULTIPLE retry for ${key}. Manual intervention required.\n`
            );
            return;
        }
    }
    await copy.copyKeyToDestinationBucket(key, statusRecord.Attributes.sz.N);
    await closeOffRecord(statusRecord);
}

async function closeOffRecord(statusRecord) {
    let key = statusRecord.Attributes.fname.S;
    await db.setTimestampNow(statusRecord.Attributes.aid.S, "vdt");
    await s3.deleteObject({
        Bucket: STAGING_BUCKET,
        Key: `${STAGING_BUCKET_PREFIX}/${key}`
    }).promise();
}

async function fileExists(Bucket, key) {
    let objects = await s3
        .listObjectsV2({
            Bucket,
            Prefix: key,
        }).promise();

    for (let r of objects.Contents) {
        console.log(r.Key);
        if (r.Key === key) {
            return r;
        }
    }

    return false;
}

// Retry when hash mismatch is found
async function failArchiveAndRetry(statusRecord, key) {
    // Delete file from STAGING bucket
    let params = {
        Bucket: STAGING_BUCKET,
        Key: key,
    };
    try {
        console.log(
            `Deleting object ${key} from Staging Bucket ${STAGING_BUCKET} after mismatch in hash comparison`
        );
        await s3.deleteObject(params).promise();
    } catch (e) {
        console.error(
            `Error deleting object ${key} from Staging Bucket ${STAGING_BUCKET} after mismatch in hash comparison`
        );
        console.error(e);
    }

    //Increment Retry Counter (rc) in db and get the new count
    const updatedItem = await db.setRetryCount(
        statusRecord.Attributes.aid.S,
        "rc"
    );
    const retryCount = parseInt(updatedItem.Attributes.rc.N);
    console.error(
        `Submitting retry copy request message on SQS. New retry counter is ${retryCount} for ${key}`
    );

    //Repost to SQS (ArchiveRetrievalNotificationQueue) archive id, job id to trigger another copy
    let messageBody = JSON.stringify({
        Message: JSON.stringify({
            Action: "RetryRequest",
            JobId: statusRecord.Attributes.jobId.S,
            ArchiveId: statusRecord.Attributes.aid.S,
            ArchiveSizeInBytes: parseInt(statusRecord.Attributes.sz.N), // ParseInt to convert
        }),
    });

    let hashQueueUrl = await sqs
        .getQueueUrl({
            QueueName: SQS_ARCHIVE_NOTIFICATION,
        })
        .promise();

    return await sqs
        .sendMessage({
            QueueUrl: hashQueueUrl.QueueUrl,
            MessageBody: messageBody,
        })
        .promise();
}

module.exports = {
    handler
}
