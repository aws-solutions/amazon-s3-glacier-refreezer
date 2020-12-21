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

const parameterParser = require('./lib/parameterParser')
const storageService = require('./lib/storageService')
const glacierService = require('./lib/glacierService')
const cloudformation = require('./lib/cloudformation')

const {
    SOURCE_VAULT,
    DESTINATION_BUCKET,
    SNS_TOPIC_ARN,
    FILELIST_LOCATION,
    SNS_VAULT_CONF,
    CLOUDTRAIL_EXPORT_CONF
} = process.env;

async function handler(event, context) {
    console.log(`${JSON.stringify(event)}`)
    let responseData = {}

    //------------------------------------------------------------------------
    // [ ON DELETE ]
    if (event.RequestType == 'Delete') {
        console.log('Cleaning up staging bucket: started')
        // Cleaning up staging bucket folders except the "staging".
        // If any of the archives have not been copied over to the destination bucket the folder will be present
        // and the bucket deletion by cloudformation will fail, preserving the data for investigation
        await storageService.cleanupStagingBucket()
        responseData = {message: 'Cleaning up staging bucket: completed'};
        console.log(responseData.message)
        await cloudformation.sendResponse(event, context, "SUCCESS", responseData)
        return
    }

    //------------------------------------------------------------------------
    // [ ON CREATE ]
    const jobName = SOURCE_VAULT + '_' + "request_inventory_job_" + Date.now();

    // validating the cost impacting acknowledgements
    if (!parameterParser.isValidParameter("Yes", CLOUDTRAIL_EXPORT_CONF) ||
        !parameterParser.isValidParameter("Yes", SNS_VAULT_CONF)) {
        responseData = {message: "You can only run this cloudformation template with (1). single CloudTrail export to S3 (2). acceptance that SNS notification topic on the vault has been disabled, or it is okay to receive notification for ALL files in the vault."};
        console.error(responseData.message)
        await cloudformation.sendResponse(event, context, "FAILED", responseData)
        return
    }

    // validating key parameters
    if (!parameterParser.checkRquiredParameter(SOURCE_VAULT) ||
        !parameterParser.checkRquiredParameter(SNS_TOPIC_ARN) ||
        !parameterParser.checkRquiredParameter(DESTINATION_BUCKET)) {
        responseData = {message: "The SOURCE_VAULT, SNS_TOPIC or DESTINATION_BUCKET is missing. Check if the environment variables have been set."};
        console.error(responseData.message)
        await cloudformation.sendResponse(event, context, "FAILED", responseData)
        return
    }

    // Checking access to the target bucket
    const bucketAccessible = await storageService.checkBucketExists(DESTINATION_BUCKET);
    if (!bucketAccessible) {
        responseData = {message: "The destination bucket does not exist or is not accessible. Validate the destination bucket and its permissions before running again."};
        console.error(responseData.message)
        await cloudformation.sendResponse(event, context, "FAILED", responseData)
        return;
    }

    // Copy archive description override file, if exists
    if (FILELIST_LOCATION &&
        FILELIST_LOCATION.trim() != "") {
        await storageService.copyFilelist(FILELIST_LOCATION)
    }

    console.log(`Inventory Topic : ${SNS_TOPIC_ARN}`);

    // Glacier API call to Request inventory
    var params = {
        accountId: "-",
        jobParameters: {
            Description: jobName,
            Format: "CSV",
            SNSTopic: SNS_TOPIC_ARN,
            Type: "inventory-retrieval"
        },
        vaultName: SOURCE_VAULT
    };
    const data = await glacierService.startJob(params);
    console.log(`Output from glacier request inventory call ${JSON.stringify(data)}`);

    if (data.location && data.jobId) {
        responseData = { message: `Glacier Inventory Job has been started successfully: ${data.jobId}` };
        console.log(responseData)
        await cloudformation.sendResponse(event, context, "SUCCESS", responseData);
    } else {
        responseData = {message: `ERROR: Lambda Function to Invoke the Inventory Retrieval Job has failed ${JSON.stringify(data)}`};
        console.error(responseData.message)
        await cloudformation.sendResponse(event, context, "FAILED", responseData)
    }
}

module.exports = {
    handler
};
