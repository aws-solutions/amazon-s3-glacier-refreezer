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

const dynamo = require('./lib/dynamo.js');
const metrics = require('./lib/metrics.js');

async function handler() {

    const progressCount = await dynamo.getItem('count');
    const progressVolume = await dynamo.getItem('volume');
    const throttling = await dynamo.getItem('throttling');
    const failedArchives = await dynamo.getItem('archives-failed');

    let totalCount = progressCount && progressCount.total ? parseInt(progressCount.total.N) : null;
    let requestedCount = progressCount && progressCount.requested ? parseInt(progressCount.requested.N) : 0;
    let stagedCount = progressCount && progressCount.staged ? parseInt(progressCount.staged.N) : 0;
    let validatedCount = progressCount && progressCount.validated ? parseInt(progressCount.validated.N) : 0;
    let copiedCount = progressCount && progressCount.copied ? parseInt(progressCount.copied.N) : 0;

    let totalBytes = progressVolume && progressVolume.total ? parseInt(progressVolume.total.N) : null;
    let requestedBytes = progressVolume && progressVolume.requested ? parseInt(progressVolume.requested.N) : 0;
    let stagedBytes = progressVolume && progressVolume.staged ? parseInt(progressVolume.staged.N) : 0;
    let validatedBytes = progressVolume && progressVolume.validated ? parseInt(progressVolume.validated.N) : 0;
    let copiedBytes = progressVolume && progressVolume.copied ? parseInt(progressVolume.copied.N) : 0;

    let totalThrottledBytes = throttling && throttling.throttledBytes ? parseInt(throttling.throttledBytes.N) : null;
    let totalThrottlingErrorCount = throttling && throttling.errorCount ? parseInt(throttling.errorCount.N) : null;
    let totalFailedArchivesBytes = failedArchives && failedArchives.failedBytes ? parseInt(failedArchives.failedBytes.N) : null;
    let totalFailedArchivesErrorCount = failedArchives && failedArchives.errorCount ? parseInt(failedArchives.errorCount.N) : null;

    let metricList = [];

    if (totalCount !== null) {
        requestedCount = requestedCount > totalCount ? totalCount : requestedCount;
        stagedCount = stagedCount > totalCount ? totalCount : stagedCount;
        validatedCount = validatedCount > totalCount ? totalCount : validatedCount;
        copiedCount = copiedCount > totalCount ? totalCount : copiedCount;

        requestedBytes = requestedBytes > totalBytes ? totalBytes : requestedBytes;
        stagedBytes = stagedBytes > totalBytes ? totalBytes : stagedBytes;
        validatedBytes = validatedBytes > totalBytes ? totalBytes : validatedBytes;
        copiedBytes = copiedBytes > totalBytes ? totalBytes : copiedBytes;

        metricList.push({metricName: "ArchiveCountTotal", metricValue: totalCount});
        metricList.push({metricName: "BytesTotal", metricValue: totalBytes});
    }

    if (totalThrottlingErrorCount !== null) {
        metricList.push({metricName: "ThrottledBytes", metricValue: totalThrottledBytes});
        metricList.push({metricName: "ThrottledErrorCount", metricValue: totalThrottlingErrorCount});
    }

    if (totalFailedArchivesErrorCount !== null) {
        metricList.push({metricName: "FailedArchivesBytes", metricValue: totalFailedArchivesBytes});
        metricList.push({metricName: "FailedArchivesErrorCount", metricValue: totalFailedArchivesErrorCount});
    }

    metricList.push({metricName: "ArchiveCountRequested", metricValue: requestedCount});
    metricList.push({metricName: "ArchiveCountStaged", metricValue: stagedCount});
    metricList.push({metricName: "ArchiveCountValidated", metricValue: validatedCount});
    metricList.push({metricName: "ArchiveCountCompleted", metricValue: copiedCount});
    metricList.push({metricName: "BytesRequested", metricValue: requestedBytes});
    metricList.push({metricName: "BytesStaged", metricValue: stagedBytes});
    metricList.push({metricName: "BytesValidated", metricValue: validatedBytes});
    metricList.push({metricName: "BytesCompleted", metricValue: copiedBytes});

    await metrics.publishMetric(metricList);
}

module.exports = {
    handler
};
