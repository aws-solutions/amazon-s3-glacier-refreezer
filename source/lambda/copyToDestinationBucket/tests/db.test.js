// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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

// (Optional) Keep test output free of error messages printed by our lambda function
sinon.stub(console, "error");

describe("-- Copy To Destination Bucket Test --", () => {
    describe("-- db Test --", () => {
        var AWS;
        var db;
        var updateItemFunc;
        var getItemFunc;
        var statusTableItems;
        var result;
        var validArchiveId =
            "-_27G6RJ0mYFtcF4dF9_eWRPYFkndowEpxodhax26-t9UXFI-AaEZszxf80pu_4JCPvOGMIUA933I80uqRX9eZBhQN8umpBt1GXZUNeUGJKgYGJwA41cwqz7hFe4W5FZQoBMEpEdQA";
        var originalDateVal = "2020-01-01T01:00:24+00:00";
        //Init
        before(function () {
            updateItemFunc = sinon.stub();
            getItemFunc = sinon.stub();

            AWS = {
                DynamoDB: sinon.stub().returns({
                    updateItem: updateItemFunc,
                    getItem: getItemFunc,
                }),
            };

            statusTableItems = {
                Items: [
                    {
                        aid: { S: validArchiveId },
                        cc: 1,
                        cdt: "2020-08-23T10:18:22+00:00",
                        sgt: "2020-08-23T14:00:24+00:00",
                        descr: "data01/Objectv40682",
                        fname: { S: "data01/Objectv40682" },
                        ifn: 2683,
                        pid: 0,
                        psdt: "2020-08-23T14:00:24+00:00",
                        sha: "40debdebbb7e575f781c8d90d4fa78de1b7cc6723988644db8e4271ef863f079",
                        sz: 1048576,
                        vdt: { S: originalDateVal },
                    },
                ],
            };

            updateItemFunc
                .withArgs(
                    sinon.match(function (param) {
                        result = statusTableItems.Items.filter((itm) => itm["aid"]["S"] === param.Key.aid.S);
                        result[0]["vdt"]["S"] = Date.now.toString();
                        return true;
                    })
                )
                .returns({
                    promise: () => result,
                });

            getItemFunc
                .withArgs(
                    sinon.match(function (param) {
                        result = statusTableItems.Items.filter((itm) => itm["aid"]["S"] === param.Key.aid.S);
                        return true;
                    })
                )
                .returns({
                    promise: () => result,
                });

            //getStatusRecord
            getItemFunc
                .withArgs(
                    sinon.match(function (param) {
                        result = statusTableItems.Items.filter((itm) => itm["aid"]["S"] === param.Key.aid.S);
                        return !param.ProjectionExpression && param.ProjectionExpression !== "fname";
                    })
                )
                .returns({
                    promise: () => result,
                });

            // Overwrite internal references with mock proxies
            db = proxyquire("../lib/db.js", {
                "aws-sdk": AWS,
            });
        });

        //Tests
        describe("-- getStatusRecord --", () => {
            it("Should RETURN valid record from DynamoDB", async () => {
                const response = await db.getStatusRecord(validArchiveId);
                expect(response.length).to.be.equal(1);
                expect(response[0].aid.S).to.be.equal(validArchiveId);
            });
            it("Should RETURN no records from DynamoDB if an invalid ID is supplied", async () => {
                const response = await db.getStatusRecord(validArchiveId + "append-some-random-text");
                expect(response.length).to.be.equal(0);
            });
        });
        describe("-- setTimestampNow --", () => {
            it("Should UPDATE timestamp in DynamoDB for supplied valid archive id", async () => {
                await expect(db.setTimestampNow(validArchiveId, "vdt")).to.be.not.rejected;
                expect(result.length).to.be.equal(1);
                expect(result[0].vdt.S).to.not.equal(originalDateVal);
            });
            it("Should THROW an error if an ID that does not exist in the DB is supplied", async () => {
                await expect(db.setTimestampNow(validArchiveId + "append-some-random-text", "vdt")).to.be.rejectedWith(
                    "Cannot read properties of undefined (reading 'vdt')"
                );
            });
        });
    });
});
