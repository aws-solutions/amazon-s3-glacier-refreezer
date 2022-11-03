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
