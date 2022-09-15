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

'use strict';

const AWS = require("aws-sdk");
const s3 = new AWS.S3();
const dynamodb = new AWS.DynamoDB();
const glacier = new AWS.Glacier({maxRetries: 50});
const athena = new AWS.Athena();

const moment = require("moment");
const csv = require("csv-parser");
const db = require('./lib/db.js');

var parseFileName = require("./lib/filenameparser.js").parseFileName;

const CHUNK_SIZE = 4 * 1024 * 1024 * 1024;
const MIN_THROTTLING_DELAY = 960;

const {
    STAGING_BUCKET,
    STATUS_TABLE,
    VAULT,
    TIER,
    SNS_TOPIC,
    DATABASE,
    PARTITIONED_INVENTORY_TABLE,
    ATHENA_WORKGROUP,
    DQL
} = process.env;

async function handler(payload) {
    const startTime = new Date().getTime();

    // Using an array to supplement DynamoDB check for the recently updated files.
    // Just in case the GSI index has not been synced for the recently added filename
    const processed = [];

    console.log(
        `Starting partition: ${payload.nextPartition}. Last partition: ${payload.maxPartition}`
    );

    console.log(`Checking progress in DynamoDB`);
    const pid = payload.nextPartition;
    var partitionMaxProcessedFileNumber = await db.getPartitionMaxProcessedFileNumber(
        pid
    );
    console.log(`Max Processed File Number : ${partitionMaxProcessedFileNumber}`);

    var resultsCSV = await readAthenaPartition(pid);
    console.log(`Reading athena results file: ${resultsCSV}`);

    const lines = await readResultsCSV(resultsCSV);

    let processedSize = Number(0);

    for (const line of lines) {
        const {
            row_num: ifn,
            size: sz,
            archiveid: aid,
            sha256treehash: sha,
            archivedescription: descr,
            creationdate: creationdate,
        } = line;

        processedSize += sz

        if (ifn <= partitionMaxProcessedFileNumber) {
            continue;
        }

        console.log(`${ifn} : ${aid}`);
        let fname = parseFileName(aid, descr);

        // Duplicates - adding creation date suffix
        if (processed.includes(fname) || (await db.filenameExists(fname))) {
            fname += `-${creationdate}`;
        }

        console.log(`${fname}`);
        const glacierJob = await glacier
            .initiateJob({
                accountId: "-",
                jobParameters: {
                    Type: "archive-retrieval",
                    ArchiveId: aid,
                    Tier: TIER,
                    SNSTopic: SNS_TOPIC,
                },
                vaultName: VAULT,
            })
            .promise();

        const jobId = glacierJob.jobId;

        const cdt = moment().format();
        const cc = calculateNumberOfChunks(sz);
        const rc = 0; // Retry count is initiated to 0
        await dynamodb
            .putItem({
                Item: AWS.DynamoDB.Converter.marshall({
                    aid,
                    jobId,
                    ifn,
                    pid,
                    sha,
                    sz,
                    cdt,
                    descr,
                    fname,
                    cc,
                    rc,
                }),
                TableName: STATUS_TABLE,
            })
            .promise();

        processed.push(fname);
        partitionMaxProcessedFileNumber = ifn;
    }

    // Increment Processed Partition Count
    payload.nextPartition = pid + 1;

    // read throttled bytes data from DDB and add it to the calculation
    const throttledBytesItem = await db.getItem('throttling');
    if (!Object.keys(throttledBytesItem).length == 0 && 'throttledBytes' in throttledBytesItem.Item) {
        var throttledBytes = parseInt(throttledBytesItem.Item.throttledBytes.N);
        var throttledErrorCount = parseInt(throttledBytesItem.Item.errorCount.N);
    }
    else {
        var throttledBytes = 0;
        var throttledErrorCount = 0;
    }

    // Calculate timeout prior the next batch
    const dailyQuota = Number(DQL)
    const endTime = new Date().getTime();
    const timeTaken = Math.floor((endTime-startTime)/1000);

    const processedShare = (processedSize + throttledBytes) / dailyQuota
    let timeout = Math.round(86400 * processedShare) - timeTaken;
    timeout = timeout < 0 ? 0 : timeout;

    // if there are some throttled bytes but timeout is 0, set it to MIN_THROTTLING_DELAY in seconds
    if (throttledBytes > 0 && timeout === 0) {
        timeout = MIN_THROTTLING_DELAY;
    }
        
    payload.timeout = timeout;

    console.log(`Processed: ${processedSize}`);
    console.log(`Throttled Bytes: ${throttledBytes}`);
    console.log(`Processed Share: ${processedShare}`);
    console.log(`Timeout: ${timeout}`);

    // decrement throttled bytes data on DDB
    if (throttledBytes > 0) {
        await db.decrementThrottledBytes(throttledBytes, throttledErrorCount);
    }

    return payload;
}

async function readAthenaPartition(partNumber) {
    console.log("Starting query");

    const queryExecution = await athena
        .startQueryExecution({
            QueryString: `select distinct row_num, archiveid, "size", sha256treehash, creationdate, archivedescription from "${DATABASE}"."${PARTITIONED_INVENTORY_TABLE}" where part=${partNumber} order by row_num`,
            QueryExecutionContext: {
                Database: DATABASE,
            },
            ResultConfiguration: {
                OutputLocation: `s3://${STAGING_BUCKET}/results/`,
            },
            WorkGroup: ATHENA_WORKGROUP
        })
        .promise();

    const QueryExecutionId = queryExecution.QueryExecutionId;
    var runComplete = false;

    console.log(`QueryID : ${QueryExecutionId}`);

    while (!runComplete) {
        console.log("Waiting for Athena Query to complete");
        await new Promise((resolve) => setTimeout(resolve, 5000));
        const result = await athena
            .getQueryExecution({QueryExecutionId})
            .promise();
        if (!["QUEUED", "RUNNING", "SUCCEEDED"].includes(result.QueryExecution.Status.State)){
            console.error(`${JSON.stringify(result)}`)
            throw "Athena exception";
        }
        runComplete = result.QueryExecution.Status.State === "SUCCEEDED";
    }

    return `results/${QueryExecutionId}.csv`;
}

function readResultsCSV(key) {
    return new Promise((resolve) => {
        const lines = [];
        s3.getObject({
            Bucket: STAGING_BUCKET,
            Key: key,
        })
            .createReadStream()
            .pipe(csv())
            .on("data", (data) => {
                data["size"] = parseInt(data["size"]);
                data["row_num"] = parseInt(data["row_num"]);
                lines.push(data);
            })
            .on("end", () => {
                console.log(`Length : ${lines.length}`);
                resolve(lines);
            });
    });
}

function calculateNumberOfChunks(sizeInBytes) {
    let numberOfChunks = Math.floor(sizeInBytes / CHUNK_SIZE);
    if (sizeInBytes % CHUNK_SIZE !== 0) {
        numberOfChunks++;
    }
    return numberOfChunks;
}

module.exports = {
    handler,
    readAthenaPartition
};
