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

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
const sinon = require("sinon");
const proxyquire = require("proxyquire").noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

describe("-- Request Archives Test --", () => {
    describe("-- Request Archives : DB Test --", () => {
        var AWS;

        var db;

        var queryFunc;

        var validArchiveId =
            "-_27G6RJ0mYFtcF4dF9_eWRPYFkndowEpxodhax26-t9UXFI-AaEZszxf80pu_4JCPvOGMIUA933I80uqRX9eZBhQN8umpBt1GXZUNeUGJKgYGJwA41cwqz7hFe4W5FZQoBMEpEdQA";

        //Init
        before(function () {
            queryFunc = sinon.stub();

            AWS = {
                DynamoDB: sinon.stub().returns({
                    query: queryFunc,
                }),
            };
        });

        describe("-- getPartitionMaxProcessedIfn --", () => {
            const expectedIFN = 2683;
            var statusIndexTableItems;

            //Init
            before(function () {
                //Matchers
                statusIndexTableItems = {
                    Items: [
                        {
                            aid: { S: validArchiveId },
                            ifn: { N: expectedIFN },
                        },
                    ],
                };

                queryFunc.withArgs(sinon.match.any).returns({
                    promise: () => statusIndexTableItems,
                });

                // Overwrite internal references with mock proxies
                db = proxyquire("../lib/db.js", {
                    "aws-sdk": AWS,
                });
            });

            //Test
            it("Should RETURN max processed file number value from DynamoDB as integer", async () => {
                const payload = {
                    currentPartition: 100,
                    maxPartition: 1000,
                };
                const response = await db.getPartitionMaxProcessedFileNumber(payload.currentPartition);
                expect(response).to.be.equal(expectedIFN);
            });
        });
    });
});
