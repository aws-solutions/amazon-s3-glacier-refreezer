// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const AWS = require("aws-sdk");
const sqs = new AWS.SQS();

const { SQS_HASH } = process.env;

const CHUNK_SIZE = 4 * 1024 * 1024 * 1024;

exports.calcHash = async (statusRecord) => {
    let key = statusRecord.Attributes.fname.S;
    let aid = statusRecord.Attributes.aid.S;
    let cc = parseInt(statusRecord.Attributes.cc.N);

    console.log(`${key} : submitting treehash calc requests`);

    let hashQueueUrl = await sqs
        .getQueueUrl({
            QueueName: SQS_HASH,
        })
        .promise();

    let i = 1;
    while (i < cc) {
        let startByte = (i - 1) * CHUNK_SIZE;
        let endByte = startByte + CHUNK_SIZE - 1;
        await sendTreeHashMessage(hashQueueUrl.QueueUrl, aid, key, i, startByte, endByte);
        i++;
    }

    // Last chunk
    let startByte = (i - 1) * CHUNK_SIZE;
    let endByte = statusRecord.Attributes.sz.N - 1;
    await sendTreeHashMessage(hashQueueUrl.QueueUrl, aid, key, i, startByte, endByte);
};

const sendTreeHashMessage = (queueUrl, aid, key, partNo, startByte, endByte) => {
    let params = {
        aid,
        key,
        partNo,
        startByte,
        endByte,
    };
    let messageBody = JSON.stringify(params);
    return sqs
        .sendMessage({
            QueueUrl: queueUrl,
            MessageBody: messageBody,
        })
        .promise();
};
