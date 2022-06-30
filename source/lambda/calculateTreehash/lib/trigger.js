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

'use strict';

const AWS = require('aws-sdk');
const sqs = new AWS.SQS();
const s3 = new AWS.S3();

const {
    DESTINATION_BUCKET,
    STORAGE_CLASS,
    SQS_COPY_TO_DESTINATION_NOTIFICATION
} = process.env;

const CHUNK_SIZE = 4 * 1024 * 1024 * 1024

exports.triggerCopyToDestinationBucket = async (statusRecord) => {

    let key = statusRecord.Attributes.fname.S;
    let aid = statusRecord.Attributes.aid.S;
    let numberOfParts = parseInt(statusRecord.Attributes.cc.N);
    let size = parseInt(statusRecord.Attributes.sz.N);

    console.log(`${key} : trigger sending messages to copyToDestinationQueue`)
    console.log(` Number of parts : ${numberOfParts} `)

    let queueUrl = await sqs.getQueueUrl({ QueueName: SQS_COPY_TO_DESTINATION_NOTIFICATION }).promise();

    const file = await fileExists(DESTINATION_BUCKET, key);
    if (file) {
        console.error(`${key} : already exists in the target bucket. Not overwriting: ${file.StorageClass}`);
        return;
    }

    if (numberOfParts > 1) {
        console.log(`Starting multipart copy for : ${key} : parts : ${numberOfParts}`)

        let multiPartUpload = await s3
            .createMultipartUpload({
                Bucket: DESTINATION_BUCKET,
                StorageClass: STORAGE_CLASS,
                Key: key
            })
            .promise();

        let partNo = 1;
        while (partNo < numberOfParts) {
            let startByte = (partNo - 1) * CHUNK_SIZE;
            let endByte = startByte + CHUNK_SIZE - 1;

            await sendMessageToCopyQueue(
                queueUrl.QueueUrl,
                aid,
                multiPartUpload.UploadId,
                key,
                partNo,
                startByte,
                endByte
            );
            partNo++;
        }

        // console.log("Processing last chunk: " + i);
        let startByte = (partNo - 1) * CHUNK_SIZE;
        let endByte = size - 1;

        await sendMessageToCopyQueue(
            queueUrl.QueueUrl,
            aid,
            multiPartUpload.UploadId,
            key,
            partNo,
            startByte,
            endByte
        );
    } else {

        console.log(`Starting single part copy for : ${key}`)
        await sendMessageToCopyQueue(
            queueUrl.QueueUrl,
            aid,
            null,
            key,
            null,
            null,
            null
        );
    }

    function sendMessageToCopyQueue(queueUrl, aid, uploadId, key, partNo, startByte, endByte) {
        let messageBody = JSON.stringify({
            aid,
            uploadId,
            key,
            partNo,
            startByte,
            endByte
        });

        return sqs.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: messageBody,
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
}