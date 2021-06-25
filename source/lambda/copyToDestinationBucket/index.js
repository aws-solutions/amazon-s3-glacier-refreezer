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
    DESTINATION_BUCKET,
    STAGING_BUCKET,
    STAGING_BUCKET_PREFIX,
} = process.env;

async function handler(event) {
    let {key, aid} = JSON.parse(event.Records[0].body);

    const file = await fileExists(DESTINATION_BUCKET, key);
    if (file) {
        console.error(`${key} : already exists in the target bucket. Not overwriting: ${file.StorageClass}`);
        return;
    }

    console.log(`${key} : copy started`);

    let statusRecord = await db.getStatusRecord(aid);
    await copy.copyKeyToDestinationBucket(key, parseInt(statusRecord.Item.sz.N));
    await closeOffRecord(statusRecord);
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

async function closeOffRecord(statusRecord) {
    let key = statusRecord.Item.fname.S;
    await db.setTimestampNow(statusRecord.Item.aid.S, "cpt");
    await s3.deleteObject({
        Bucket: STAGING_BUCKET,
        Key: `${STAGING_BUCKET_PREFIX}/${key}`
    }).promise();
}

module.exports = {
    handler
}
