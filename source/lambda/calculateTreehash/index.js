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
const s3 = new AWS.S3();
const sqs = new AWS.SQS();

const db = require("./lib/db.js");
const treehash = require("./lib/treehash.js");
const trigger = require("./lib/trigger.js");

const {
    STAGING_BUCKET,
    SQS_ARCHIVE_NOTIFICATION,
} = process.env;

async function handler(event) {
    let {aid, key, partNo, startByte, endByte} = JSON.parse(event.Records[0].body);

    console.log(`${key} - ${partNo} hash : ${startByte}-${endByte}`);

    // the only way vdt is present at this section of the code is if the treehash is successfully validated already, but 
    // triggerCopyToDestinationBucket fails, hence retry triggerCopyToDestinationBucket.
    let resultRecord = await db.getStatusRecord(aid);
    
    if (resultRecord.Item.vdt && resultRecord.Item.vdt.S) {
        resultRecord.Attributes = resultRecord.Item;
        console.log(`${key} : treehash has already been processed. Skipping`);
        await trigger.triggerCopyToDestinationBucket(resultRecord);
        return;
    }

    let cc = parseInt(resultRecord.Item.cc.N);

    let chunkHash = await treehash.getChunkHash(key, partNo, startByte, endByte);

    // Single Chunk
    if (cc === 1) {
        resultRecord.Attributes = resultRecord.Item;
        await validateTreehash(chunkHash, resultRecord);
        return;
    }

    // Multi Chunk
    let statusRecord = await db.updateChunkStatusGetLatest(aid, partNo, chunkHash);

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

    let multiPartHash = treehash.calculateMultiPartHash(statusRecord);

    await validateTreehash(multiPartHash, statusRecord);
}

async function validateTreehash(s3hash, statusRecord) {
    let key = statusRecord.Attributes.fname.S;
    let glacierHash = statusRecord.Attributes.sha.S;

    if (s3hash !== glacierHash) {
        console.error(`ERROR : sha256treehash validation failed : ${key}.\n`);
        console.log(`SHA256TreeHash Glacier : ${glacierHash}.`);
        console.log(`SHA256TreeHash S3      : ${s3hash}.`);

        let retryCount = parseInt(statusRecord.Attributes.rc.N);
        if (retryCount < 3) {
            console.error(
                `ERROR : sha256treehash validation failed for ${key}. Retry: ${retryCount}.\n`
            );
            await failArchiveAndRetry(statusRecord, key);
            return;
        } else {
            // on 3rd failed calcHash attempt, we log in db
            await db.increaseArchiveFailedBytesAndErrorCount("archives-failed", "failedBytes", statusRecord.Attributes.sz.N, "errorCount", "1")
            console.error(
                `ERROR : sha256treehash validation failed after MULTIPLE retry for ${key}. Manual intervention required.\n`
            );
            return;
        }
    }
    // now that TreeHash is successfully verified, we start the copy to destination bucket via sending a message to the SQS
   
    // setTimestampNow runs here because we want to mark the time when the validation was completed
    await db.setTimestampNow(statusRecord.Attributes.aid.S, "vdt");

    await trigger.triggerCopyToDestinationBucket(statusRecord);

    }

async function getListOfChunks(statusRecord){
    let cc = parseInt(statusRecord.Attributes.cc.N);
    var chunkString = "chunk";
    var finalChunkString= "";
    for (var chunkNumber = 1; chunkNumber < cc ; chunkNumber ++){
        finalChunkString = finalChunkString.concat(chunkString.concat(chunkNumber.toString()).concat(", "));
    }
    finalChunkString = finalChunkString.concat(chunkString.concat(cc.toString()));
    return finalChunkString
}


async function failArchiveAndRetry(statusRecord, key) {
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

    const updatedItem = await db.incrementRetryCount(
        statusRecord.Attributes.aid.S,
        "rc"
    );
    const retryCount = parseInt(updatedItem.Attributes.rc.N);
    console.error(
        `Submitting retry copy request message on SQS. New retry counter is ${retryCount} for ${key}`
    );

    // wipe sgt and chunks' status and start over
    await db.deleteItem(statusRecord.Attributes.aid.S, "sgt");
    console.log(`${key} : sgt deleted`);
    await db.deleteChunkStatus(statusRecord.Attributes.aid.S, await getListOfChunks(statusRecord));

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
