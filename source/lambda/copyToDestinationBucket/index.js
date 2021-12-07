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

const db = require("./lib/db.js");
const copy = require("./lib/copy.js");

const {
    DESTINATION_BUCKET
} = process.env;

async function handler(event) {
    let {
        key,
        aid,
        uploadId,
        partNo,
        startByte,
        endByte
    } = JSON.parse(event.Records[0].body);

    const file = await fileExists(DESTINATION_BUCKET, key);
    if (file) {
        console.log(`${key} : already copied to the target bucket. Possible duplicated SQS message. No action is required.`);
        return;
    }

    await copy.copyKeyToDestinationBucket(key, aid, uploadId, partNo, startByte, endByte);
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

module.exports = {
    handler
}
