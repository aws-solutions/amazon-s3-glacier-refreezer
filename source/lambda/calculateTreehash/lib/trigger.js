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

const AWS = require('aws-sdk');
const sqs = new AWS.SQS();

const {
    SQS_COPY_TO_DESTINATION_NOTIFICATION
} = process.env;

const CHUNK_SIZE = 4 * 1024 * 1024 * 1024

exports.triggerCopyToDestinationBucket = async (statusRecord) => {

    let key = statusRecord.Attributes.fname.S;
    let aid = statusRecord.Attributes.aid.S;

    console.log(`${key} : trigger sending messages to copyToDestinationQueue`)


    let queueUrl = await sqs.getQueueUrl({ QueueName: SQS_COPY_TO_DESTINATION_NOTIFICATION }).promise();
    await sendMessageToCopyQueue(
        queueUrl.QueueUrl,
        key,
        statusRecord.Attributes.aid.S
    );

    function sendMessageToCopyQueue(queueUrl, key, aid) {
        let messageBody = JSON.stringify({
            key,
            aid,
        });
        
        return sqs.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: messageBody,
        }).promise();
    };
}