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

const AWS = require('aws-sdk');
const S3 = new AWS.S3();

const CryptoJS = require('crypto-js')
const crypto = require('crypto');

const {
    STAGING_BUCKET,
    STAGING_BUCKET_PREFIX
} = process.env;

const ONE_MB = 1024 * 1024;

async function getChunkHash(key, chunk, startByte, endByte) {

    let chunkHashes = await getChunkOneMBHashes(key, startByte, endByte);
    let hash = computeSHA256TreeHash(chunkHashes)
    console.log(`${key} chunk hash : ${chunk} : ${hash}`)
    return hash
}

async function getChunkOneMBHashes(key, rangeStart, rangeEnd) {

    let leafHashes = []
    let readStream = S3.getObject({
        Bucket: STAGING_BUCKET,
        Key:    `${STAGING_BUCKET_PREFIX}/${key}`,
        Range:  `bytes=${rangeStart}-${rangeEnd}`
    }).createReadStream()

    let buffer = ""
    let watermark = 0

    for await (const chunk of readStream) {

        let newMark = watermark + chunk.length

        if (newMark < ONE_MB) {
            buffer += chunk.toString('binary')
            watermark = newMark;
            continue;
        }

        if (newMark === ONE_MB) {
            buffer += chunk.toString('binary')

            const hash = crypto.createHash('sha256');
            hash.update(Buffer.from(buffer, 'binary'), input_encoding = 'binary');

            const hashValue = hash.digest('hex')
            leafHashes.push(hashValue);

            watermark = 0
            buffer = ""
            continue
        }

        if (newMark > ONE_MB) {
            let remainder = ONE_MB - watermark;
            let newWatermark = (chunk.length - remainder);

            let this_buffer = buffer + chunk.slice(0, remainder).toString('binary')
            let next_buffer = chunk.slice(remainder, remainder + newWatermark).toString('binary')

            const hash = crypto.createHash('sha256');
            hash.update(Buffer.from(this_buffer, 'binary'), input_encoding = 'binary');
            const hashValue = hash.digest('hex')
            leafHashes.push(hashValue);

            buffer = next_buffer
            watermark = newWatermark;
            continue
        }
    }

    if (buffer.length > 0) {
        const hash = crypto.createHash('sha256');
        hash.update(Buffer.from(buffer, 'binary'), input_encoding = 'binary');
        const hashValue = hash.digest('hex')
        leafHashes.push(hashValue);
    }
    return leafHashes
}

function computeSHA256TreeHash(chunkHashes) {

    let prevLvlHashes = [];
    for (const entry of chunkHashes) {
        prevLvlHashes.push(CryptoJS.enc.Hex.parse(entry))
    }

    while (prevLvlHashes.length > 1) {
        let currLvlHashes = []
        for (let i = 0; i < prevLvlHashes.length; i = i + 2) {

            // If there are at least two elements remaining
            if (prevLvlHashes.length - i > 1) {
                let entry = prevLvlHashes[i].clone().concat(prevLvlHashes[i + 1])
                let entryHash = CryptoJS.SHA256(entry)
                currLvlHashes.push(entryHash)
            } else { // Take care of remaining odd chunk
                currLvlHashes.push(prevLvlHashes[i])
            }
        }
        prevLvlHashes = currLvlHashes
    }
    return prevLvlHashes[0].toString(CryptoJS.enc.Hex)
}

function calculateMultiPartHash(statusRecord) {
    let cc = parseInt(statusRecord.Attributes.cc.N);

    let hashes = [];

    Array.from(Array(cc)).forEach((_, i) => {
        const chunkId = i + 1;
        let entry = statusRecord.Attributes[`chunk${chunkId}`].S;
        hashes.push(entry);
    });

    return computeSHA256TreeHash(hashes);
};

module.exports = {
    getChunkHash,
    calculateMultiPartHash
};
