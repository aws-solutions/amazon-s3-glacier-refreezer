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

const dynamo = require('./lib/dynamo.js');
const metrics = require('./lib/metrics.js');

async function handler() {

    const progressCount = await dynamo.getCount();
    let restored =  await metrics.getNumberOfMessagesPublishedIn30Days();

    let total = progressCount && progressCount.total ? progressCount.total.N : null;
    let requested = progressCount && progressCount.requested ? progressCount.requested.N : 0;
    let staged = progressCount && progressCount.staged ? progressCount.staged.N : 0;
    let validated = progressCount && progressCount.validated ? progressCount.validated.N : 0;
    let copied = progressCount && progressCount.copied ? progressCount.copied.N : 0;

    if (total) {
        requested = requested > total ? total : requested;
        restored = restored > total ? total : restored;
        staged = staged > total ? total : staged;
        validated = validated > total ? total : validated;
        copied = copied > total ? total : copied;
    }

    await metrics.publishMetric('Total Archives', total);
    await metrics.publishMetric('Requested from Glacier', requested);
    await metrics.publishMetric('Restored', restored);
    await metrics.publishMetric('Staged', staged);
    await metrics.publishMetric('Hashes Validated', validated);
    await metrics.publishMetric('Copied to Destination', copied);
}

module.exports = {
    handler
};
