// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const db = require("./db.js");

const { DESTINATION_BUCKET, STORAGE_CLASS, STAGING_BUCKET, STAGING_BUCKET_PREFIX } = process.env;

async function copyKeyToDestinationBucket(key, aid, uploadId, partNo, startByte, endByte) {
    const currentStorageClass = await this.getKeyStorageClass(key);
    // Staging bucket object must always be of STANDARD storage class
    // If the entry does not exist - the file does not exist
    if (!currentStorageClass) {
        console.error(`${key} : 
        Unable to copy to the destination bucket as staging bucket doesn't have the specified file. 
        Can be false positive, when invoked on retry. 
        Please check the destination bucket.`);
    }

    if (uploadId == null) {
        console.log(`Single part copy  : ${key} : ${STORAGE_CLASS}`);
        await s3
            .copyObject({
                CopySource: encodeURIComponent(`${STAGING_BUCKET}/${STAGING_BUCKET_PREFIX}/${key}`),
                Bucket: DESTINATION_BUCKET,
                Key: key,
                StorageClass: STORAGE_CLASS,
            })
            .promise();

        await closeOffRecord(aid, key);
        console.log(`Copy completed : ${key}`);
    } else {
        console.log(`Multi part copy for : ${key} - ${partNo}`);
        await copyMultiPart(key, aid, uploadId, partNo, startByte, endByte);
    }
}

async function copyPart(uploadId, key, partNo, startByte, endByte) {
    return s3
        .uploadPartCopy({
            UploadId: uploadId,
            CopySource: encodeURIComponent(`${STAGING_BUCKET}/${STAGING_BUCKET_PREFIX}/${key}`),
            Bucket: DESTINATION_BUCKET,
            Key: key,
            PartNumber: partNo,
            CopySourceRange: `bytes=${startByte}-${endByte}`,
        })
        .promise();
}

async function copyMultiPart(key, aid, uploadId, partNo, startByte, endByte) {
    let uploadResult = await copyPart(uploadId, key, partNo, startByte, endByte);

    let etag = uploadResult.ETag;
    let latestStatusRecord = await db.updateChunkStatusGetLatest(aid, partNo, etag);
    let cc = parseInt(latestStatusRecord.Attributes.cc.N);

    let count = 0;
    for (const entry in latestStatusRecord.Attributes) {
        if (
            entry.includes("chunk") &&
            latestStatusRecord.Attributes[entry].S &&
            latestStatusRecord.Attributes[entry].S.length < 64
        ) {
            count++;
        }
    }

    if (count < cc) return;

    let eTags = [];
    Array.from(Array(cc)).forEach((_, i) => {
        const chunkId = i + 1;
        let entry = latestStatusRecord.Attributes[`chunk${chunkId}`].S;
        eTags.push({
            PartNumber: chunkId,
            ETag: entry,
        });
    });

    await s3
        .completeMultipartUpload({
            Bucket: DESTINATION_BUCKET,
            Key: key,
            MultipartUpload: {
                Parts: eTags,
            },
            UploadId: uploadId,
        })
        .promise();

    await closeOffRecord(aid, key);
    console.log(`Multipart copy completed : ${key}`);
}

async function getKeyStorageClass(key) {
    let objects = await s3
        .listObjectsV2({
            Bucket: STAGING_BUCKET,
            Prefix: `${STAGING_BUCKET_PREFIX}/${key}`,
        })
        .promise();
    return objects.Contents[0].StorageClass;
}

async function closeOffRecord(aid, key) {
    await db.setTimestampNow(aid, "cpt");
    await s3
        .deleteObject({
            Bucket: STAGING_BUCKET,
            Key: `${STAGING_BUCKET_PREFIX}/${key}`,
        })
        .promise();
}

module.exports = {
    copyKeyToDestinationBucket,
    getKeyStorageClass,
};
