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

const cloudformation = require('./lib/cloudformation')

exports.handler = function(event, context) {
    console.log(JSON.stringify(event))
    let responseData = {}
    for(const [key, val] of Object.entries(event.ResourceProperties)) {
        responseData[key] = val.toLowerCase();
      }
    console.log(responseData)
    try {
      cloudformation.sendResponse(event, context, "SUCCESS", responseData);
    } catch (err) {
      console.error(err);
      cloudformation.sendResponse(event, context, "FAILED",
      {message: `${JSON.stringify(err)}`});
    }     
};
