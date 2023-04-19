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

describe("-- Copy Chunk Test --", () => {
    describe("-- trigger Test --", () => {
        var AWS;

        var trigger;

        var getQueueUrlFunc;
        var sendMessageFunc;

        var queueUrlResult;
        var sendMessageResult;
        var singleDBresult;
        //Init
        before(function () {
            getQueueUrlFunc = sinon.stub();
            sendMessageFunc = sinon.stub();

            AWS = {
                SQS: sinon.stub().returns({
                    getQueueUrl: getQueueUrlFunc,
                    sendMessage: sendMessageFunc,
                }),
            };

            //Matchers
            queueUrlResult = {
                QueueUrl: "https://sqs.ap-southeast-2.amazonaws.com/111122223333/glacier-stack-XXX-treehash-calc-queue",
            };
            sendMessageResult = {
                MD5OfMessageBody: "51b0a325...39163aa0",
                MD5OfMessageAttributes: "00484c68...59e48f06",
                MessageId: "da68f62c-0c07-4bee-bf5f-7e856EXAMPLE",
            };
            singleDBresult = {
                Attributes: {
                    fname: {
                        S: "test-key-name",
                    },
                    aid: {
                        S: "archive-id",
                    },
                    cc: {
                        N: "10",
                    },
                    sz: {
                        N: "50",
                    },
                },
            };
            getQueueUrlFunc.withArgs(sinon.match.any).returns({
                promise: () => queueUrlResult,
            });
            sendMessageFunc.withArgs(sinon.match.any).returns({
                promise: () => sendMessageResult,
            });
            // Overwrite internal references with mock proxies
            trigger = proxyquire("../lib/trigger.js", {
                "aws-sdk": AWS,
            });
        });

        //Test
        it("should successfully send the messages through SQS", async () => {
            await expect(trigger.calcHash(singleDBresult)).to.be.not.rejected; //TODO: Original method doesnt return, perhaps await issue
        });
    });
});
