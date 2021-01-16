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

const AWS = require('aws-sdk');
const s3 = new AWS.S3();

const {
    DESTINATION_BUCKET,
    STORAGE_CLASS,
    STAGING_BUCKET,
    STAGING_BUCKET_PREFIX
} = process.env;

const CHUNK_SIZE = 2 * 1024 * 1024 * 1024

async function copyKeyToDestinationBucket(key, size) {

    const currentStorageClass = await this.getKeyStorageClass(key)
    // Staging bucket object must always be of STANDARD storage class
    // If the entry does not exist - the file does not exist
    if (!currentStorageClass) {
        console.error(`${key} : 
        Unable to copy to the destination bucket as staging bucket doesn't have the specified file. 
        Can be false positive, when invoked on retry. 
        Please check the destination bucket.`);
    }

    if (size < CHUNK_SIZE) {
        console.log(`Single part Storage Class change for : ${key} : ${STORAGE_CLASS}`)
        await s3.copyObject({
            CopySource: encodeURIComponent(`${STAGING_BUCKET}/${STAGING_BUCKET_PREFIX}/${key}`),
            Bucket: DESTINATION_BUCKET,
            Key: key,
            StorageClass: STORAGE_CLASS
        }).promise();
    } else {
        await copyMultiPart(key, size)
    }
}

function copyPart (uploadId, key, partNo, startByte, endByte) {
    console.log(`${key} - ${partNo}`)
    return s3.uploadPartCopy({
        UploadId: uploadId,
        CopySource: encodeURIComponent(`${STAGING_BUCKET}/${STAGING_BUCKET_PREFIX}/${key}`),
        Bucket: DESTINATION_BUCKET,
        Key: key,
        PartNumber: partNo,
        CopySourceRange: `bytes=${startByte}-${endByte}`
    }).promise()
}

async function copyMultiPart(key, size) {
    console.log(`Multipart Storage Class change for : ${key}`)
    let response = await s3.createMultipartUpload({
        Bucket: DESTINATION_BUCKET,
        Key: key,
        StorageClass: STORAGE_CLASS
    }).promise();

    const uploadId = response.UploadId
    let partCopyRequests = []

    let parts = Math.ceil(size / CHUNK_SIZE)
    var i = 1

    while (i < parts) {
        const startByte = CHUNK_SIZE * (i - 1)
        const endByte = CHUNK_SIZE * i - 1
        partCopyRequests.push(copyPart(uploadId, key, i, startByte, endByte))
        i++
    }

    const startByte = CHUNK_SIZE * (i - 1)
    partCopyRequests.push(copyPart(uploadId, key, i, startByte, size - 1))

    console.log(`Starting S3 copy`)
    let data = await Promise.all(partCopyRequests)
    console.log(`S3 copy completed : ${key}`)

    let eTags = []
    data.forEach((item, index, array) => {
        eTags.push({
            PartNumber: index + 1,
            ETag: item.ETag
        })
    })

    await s3.completeMultipartUpload({
        Bucket: DESTINATION_BUCKET,
        Key: key,
        MultipartUpload: {
            Parts: eTags
        },
        UploadId: response.UploadId
    }).promise();
    console.log(`Done`)
}

async function getKeyStorageClass(key) {
    let objects = await s3.listObjectsV2({
        Bucket: STAGING_BUCKET,
        Prefix: `${STAGING_BUCKET_PREFIX}/${key}`,
    }).promise()
    return objects.Contents[0].StorageClass
}

module.exports = {
    copyKeyToDestinationBucket,
    getKeyStorageClass
};
