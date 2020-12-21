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

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const glacier = new AWS.Glacier();

async function handler(event) {

    console.log(`inventory - ${event.partNo}`)

    let inventoryStream = glacier.getJobOutput(
        {
            accountId: "-",
            jobId: event.jobId,
            range: `bytes=${event.startByte}-${event.endByte}`,
            vaultName: event.vault,
        }).createReadStream()

    inventoryStream.length = event.endByte - event.startByte + 1

    return await s3.uploadPart({
        UploadId: event.uploadId,
        Bucket: event.bucket,
        Key: event.key,
        PartNumber: event.partNo,
        Body: inventoryStream
    }).promise()
}

module.exports = {
    handler
};
