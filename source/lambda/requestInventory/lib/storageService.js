/*********************************************************************************************************************
 *  Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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
const s3 = new AWS.S3();

const { STAGING_BUCKET, STAGING_LIST_PREFIX } = process.env;

const CHUNK_SIZE = 2 * 1024 * 1024 * 1024;

async function checkBucketExists(bucketName) {
    console.log(`Checking access to the bucket: ${bucketName}`);
    if (!bucketName) {
        console.error(`The parameter bucket name is undefined`);
        return false;
    }
    try {
        await s3
            .headBucket({
                Bucket: bucketName,
            })
            .promise();
        console.log("Exists and accessible");
        return true;
    } catch (error) {
        console.error(
            `The destination bucket does not exist or is not accessible. Check the bucket name and permissions before running again.`
        );
        console.error(error);
        return false;
    }
}

async function copyFilelist(simpleUrl) {
    console.log(`External filename list location: ${simpleUrl}`);

    let bucket = simpleUrl.slice(0, simpleUrl.indexOf("/"));
    let key = simpleUrl.slice(simpleUrl.indexOf("/") + 1);
    let filename = simpleUrl.replace(/(.*\/)/g, "");

    let result = await s3
        .headObject({
            Key: key,
            Bucket: bucket,
        })
        .promise();

    let size = result.ContentLength;
    let stagingKey = `${STAGING_LIST_PREFIX}/${filename}`;

    if (size < CHUNK_SIZE) {
        console.log(`Single part copy for : ${simpleUrl}`);
        await s3
            .copyObject({
                CopySource: encodeURIComponent(`${simpleUrl}`),
                Bucket: STAGING_BUCKET,
                Key: stagingKey,
            })
            .promise();
    } else {
        await copyMultiPart(simpleUrl, stagingKey, size);
    }
}

const copyPart = (uploadId, sourceUrl, targetKey, partNo, startByte, endByte) => {
    console.log(`${targetKey} - ${partNo}`);
    return s3
        .uploadPartCopy({
            UploadId: uploadId,
            CopySource: encodeURIComponent(`${sourceUrl}`),
            Bucket: STAGING_BUCKET,
            Key: targetKey,
            PartNumber: partNo,
            CopySourceRange: `bytes=${startByte}-${endByte}`,
        })
        .promise();
};

const copyMultiPart = async (sourceUrl, targetKey, size) => {
    console.log(`Multipart copy for : ${STAGING_BUCKET}/${targetKey}`);
    let response = await s3
        .createMultipartUpload({
            Bucket: STAGING_BUCKET,
            Key: targetKey,
        })
        .promise();

    const uploadId = response.UploadId;
    let partCopyRequests = [];

    let parts = Math.ceil(size / CHUNK_SIZE);
    var i = 1;

    while (i < parts) {
        const startByte = CHUNK_SIZE * (i - 1);
        const endByte = CHUNK_SIZE * i - 1;
        partCopyRequests.push(copyPart(uploadId, sourceUrl, targetKey, i, startByte, endByte));
        i++;
    }

    const startByte = CHUNK_SIZE * (i - 1);
    partCopyRequests.push(copyPart(uploadId, sourceUrl, targetKey, i, startByte, size - 1));

    console.log(`Starting S3 copy`);
    let data = await Promise.all(partCopyRequests);
    console.log(`S3 copy completed : ${targetKey}`);

    let eTags = [];
    data.forEach((item, index, array) => {
        eTags.push({
            PartNumber: index + 1,
            ETag: item.ETag,
        });
    });

    await s3
        .completeMultipartUpload({
            Bucket: STAGING_BUCKET,
            Key: targetKey,
            MultipartUpload: {
                Parts: eTags,
            },
            UploadId: response.UploadId,
        })
        .promise();
    console.log(`Done`);
};

async function getObjectList(Bucket, Prefix) {
    const objectList = [];
    let params = { Bucket, Prefix };
    while (true) {
        const result = await s3.listObjectsV2(params).promise();
        result.Contents.forEach((entry) => objectList.push(entry.Key));
        if (!result.NextContinuationToken) break;
        params.ContinuationToken = result.NextContinuationToken;
    }
    return objectList;
}

async function cleanupStagingBucket() {
    let objectList = [];
    objectList = objectList.concat(await getObjectList(STAGING_BUCKET, "results"));
    objectList = objectList.concat(await getObjectList(STAGING_BUCKET, "partitioned"));
    objectList = objectList.concat(await getObjectList(STAGING_BUCKET, "inventory"));
    objectList = objectList.concat(await getObjectList(STAGING_BUCKET, STAGING_LIST_PREFIX));
    objectList = objectList.concat(await getObjectList(STAGING_BUCKET, "glue"));

    let delList = [];
    let count = 0;
    while (count < objectList.length) {
        delList.push({
            Key: objectList[count],
        });
        count++;
        if ((count + 1) % 1000 === 0) {
            console.log(`Deleting batch ${Math.round(count / 999)}`);
            await deleteBucketKeys(STAGING_BUCKET, delList);
            delList = [];
        }
    }
    console.log("Deleting last batch");
    await deleteBucketKeys(STAGING_BUCKET, delList);
}

async function deleteBucketKeys(Bucket, Objects) {
    console.log(`Cleaning ${Bucket}. Entry count : ${Objects.length}`);
    if (Objects.length)
        await s3
            .deleteObjects({
                Bucket,
                Delete: {
                    Objects,
                    Quiet: false,
                },
            })
            .promise();
}

module.exports = {
    checkBucketExists,
    copyFilelist,
    cleanupStagingBucket,
};
