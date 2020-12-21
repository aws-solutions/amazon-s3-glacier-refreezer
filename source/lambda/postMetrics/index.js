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
const { 
    getTotalRecords, 
    getProcessProgress, 
    publishMetric 
} = require('./lib/publishMetric.js')

async function handler() {

    const archivesTotal = await getTotalRecords();
    const processProgress = await getProcessProgress();

    let archivesRequested = processProgress && processProgress.requested ? processProgress.requested.N : 0
    let archivesInitiated = processProgress && processProgress.started ? processProgress.started.N : 0
    let archivesCompleted = processProgress && processProgress.completed ? processProgress.completed.N : 0
    let treehashValidated = processProgress && processProgress.validated ? processProgress.validated.N : 0

    if (archivesTotal) {
        archivesRequested = archivesRequested > archivesTotal ? archivesTotal : archivesRequested
        archivesInitiated = archivesInitiated > archivesTotal ? archivesTotal : archivesInitiated
        archivesCompleted = archivesCompleted > archivesTotal ? archivesTotal : archivesCompleted
        treehashValidated = treehashValidated > archivesTotal ? archivesTotal : treehashValidated
    }

    await publishMetric('Total Archives', archivesTotal);
    await publishMetric('Requested from Glacier', archivesRequested);
    await publishMetric('Copy Initiated', archivesInitiated);
    await publishMetric('Copy Completed', archivesCompleted);
    await publishMetric('Hashes Validated', treehashValidated);
}

module.exports = {
    handler
};
