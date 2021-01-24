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

const cloudformation = require('./lib/cloudformation');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
var fs = require('fs');

const {
    STAGING_BUCKET
} = process.env;

const FILE_NAME='partition-inventory.py'

async function handler(event, context) {
    console.log(JSON.stringify(event));

    //------------------------------------------------------------------------
    // [ ON CREATE ]
    if (event.RequestType === 'Create') {
        console.log('Deploying Glue Job PySpark code');
        let readStream = fs.createReadStream(__dirname+'/'+FILE_NAME);
        let copyResult = await s3
            .putObject({
                Bucket: STAGING_BUCKET,
                Key: `glue/${FILE_NAME}`,
                Body: readStream,
            }) .promise();

        console.log(JSON.stringify(copyResult));
        await cloudformation.sendResponse(event, context, "SUCCESS", {message: 'Glue Script Copied'});
        return;
    }

    await cloudformation.sendResponse(event, context, "SUCCESS", {});
}

module.exports = {
    handler
};
