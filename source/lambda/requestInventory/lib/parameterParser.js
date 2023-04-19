// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

async function checkRquiredParameter(param) {
    if (!param) return false;
    return true;
}

async function isValidParameter(desiredValue, actualValue) {
    if (!actualValue) return false;
    if (desiredValue !== actualValue) return false;
    return true;
}

module.exports = {
    checkRquiredParameter,
    isValidParameter,
};
