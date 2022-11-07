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

"use strict";

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
const { mock } = require("sinon");
const sinon = require("sinon");
const proxyquire = require("proxyquire").noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

// (Optional) Keep test output free of error messages printed by our lambda function
sinon.stub(console, "error");

describe("-- Download Inventory Test --", () => {
    var AWS;

    var getJobOutputFunc;
    var putObjectFunc;
    var startExecutionFunc;

    var invokeFunc;
    var createMultipartUploadFunc;
    var completeMultipartUploadFunc;
    var abortMultipartUploadFunc;

    var index;

    var startExecutionResult;

    const validJobId = "zbxcm3Z_3z5UkoroF7SuZKrxgGoDc3RloGduS7Eg-RO47Yc6FxsdGBgf_Q2DK5Ejh18CnTS5XW4_XqlNHS61dsO4CnMW";
    const validBucketName = "Test-Glacier-Bucket";
    const MAX_SIZE = 4 * 1024 * 1024 * 1024;
    const MULTI_PART_SIZE = 4 * 1024 * 1024 * 1024 * 2;

    //Init
    before(function () {
        process.env.INVENTORY_BUCKET = validBucketName;
        process.env.BUCKET_PREFIX = "Test";
        process.env.GLACIER_VAULT = "Test-Glacier-Vault";
        process.env.INVENTORY_PART_FUNCTION = "downloadInventoryPart";
        process.env.STAGE_TWO_SF_ARN = "StageTwoOrchestrator";

        getJobOutputFunc = sinon.stub();
        putObjectFunc = sinon.stub();
        startExecutionFunc = sinon.stub();

        invokeFunc = sinon.stub();
        createMultipartUploadFunc = sinon.stub();
        completeMultipartUploadFunc = sinon.stub();
        abortMultipartUploadFunc = sinon.stub();

        AWS = {
            Glacier: sinon.stub().returns({
                getJobOutput: getJobOutputFunc,
            }),
            S3: sinon.stub().returns({
                putObject: putObjectFunc,
                createMultipartUpload: createMultipartUploadFunc,
                completeMultipartUpload: completeMultipartUploadFunc,
                abortMultipartUpload: abortMultipartUploadFunc,
            }),
            Lambda: sinon.stub().returns({
                invoke: invokeFunc,
            }),
            StepFunctions: sinon.stub().returns({
                startExecution: startExecutionFunc,
            }),
        };

        startExecutionResult = {
            data: {
                executionArn:
                    "arn:aws:sns:ap-southeast-2:111122223333:stepfunction:21be56ed-a058-49f5-8c98-aedd2564c486",
                startDate: Date.now().toString(),
            },
            err: null,
        };
        // Overwrite internal references with mock proxies
        index = proxyquire("../index.js", {
            "aws-sdk": AWS,
        });
    });
    afterEach(() => {
        delete process.env.INVENTORY_BUCKET;
        delete process.env.BUCKET_PREFIX;
        delete process.env.GLACIER_VAULT;
        delete process.env.INVENTORY_PART_FUNCTION;
        delete process.env.STAGE_TWO_SF_ARN;
    });

    //Tests
    describe("inventorySinglePart", () => {
        before(function () {
            //Matchers
            const jobOutPutStream = Buffer.alloc(MAX_SIZE);
            jobOutPutStream.write("Binary stream output retrieved from Glacier");
            const jobOutputSuccessResult = {
                data: {
                    acceptRanges: "bytes",
                    body: jobOutPutStream,
                    contentType: "application/json",
                    status: 200,
                },
                err: null,
            };
            const putObjectSucessResult = {
                data: {
                    ETag: '"6805f2cfc46c0f04559748bb039d69ae"',
                    VersionId: "Kirh.unyZwjQ69YxcQLA8z4F5j3kJJKr",
                },
                err: null,
            };
            getJobOutputFunc
                .withArgs(
                    sinon.match(function (param) {
                        return param.jobId === validJobId;
                    })
                )
                .returns({
                    createReadStream: sinon.stub().returns(jobOutputSuccessResult),
                });

            getJobOutputFunc
                .withArgs(
                    sinon.match(function (param) {
                        return param.jobId !== validJobId;
                    })
                )
                .returns({
                    createReadStream: () => {
                        throw new Error("Job not found");
                    },
                });

            putObjectFunc.withArgs(sinon.match.any).returns({
                promise: () => putObjectSucessResult,
            });

            startExecutionFunc.withArgs(sinon.match.any).returns({
                promise: () => startExecutionResult,
            });
        });

        const event = require("./snsMessage.json");

        it("Should call inventorySinglePart when size is less than MAX_SIZE", async () => {
            await expect(index.handler(event)).to.not.be.rejected;
        });
        it("Should invoke stepFunction successfully if valid job id is supplied", async () => {
            await expect(index.handler(event)).to.not.be.rejected;
        });
        it("Should throw ERROR if invalid job id is supplied", async () => {
            let mockMsg = JSON.parse(event.Records[0].Sns.Message);
            mockMsg.JobId = "some-random-jopb-id";
            event.Records[0].Sns.Message = JSON.stringify(mockMsg);
            await expect(index.handler(event)).to.be.rejectedWith("Job not found");
        });
    });

    describe("inventoryMultiPart", () => {
        before(function () {
            //Matchers
            const createMultipartUploadSucessResult = {
                data: {
                    Bucket: "examplebucket",
                    Key: "largeobject",
                    UploadId:
                        "ibZBv_75gd9r8lH_gqXatLdxMVpAlj6ZQjEs.OwyF3953YdwbcQnMA2BLGn8Lx12fQNICtMw5KyteFeHw.Sjng--",
                },
                err: null,
            };
            createMultipartUploadFunc.withArgs(sinon.match.any).returns({
                promise: () => createMultipartUploadSucessResult,
            });

            const completeMultipartUploadSucessResult = {
                data: {
                    Bucket: "examplebucket",
                    ETag: '"4d9031c7644d8081c2829f4ea23c55f7-2"',
                    Key: "bigobject",
                    Location: "https://examplebucket.s3.<Region>.amazonaws.com/bigobject",
                },
                err: null,
            };
            completeMultipartUploadFunc.withArgs(sinon.match.any).returns({
                promise: () => completeMultipartUploadSucessResult,
            });

            const abortMultipartUploadSucessResult = {
                data: {
                    RequestCharged: '"4d9031c7644d8081c2829f4ea23c55f7-2"',
                },
                err: null,
            };
            abortMultipartUploadFunc.withArgs(sinon.match.any).returns({
                promise: () => abortMultipartUploadSucessResult,
            });

            const invokePayload = { ETag: "4d9031c7644d8081c2829f4ea23c55f7-2" };
            const invokeLambdaSuccessResult = {
                Payload: JSON.stringify(invokePayload),
                StatusCode: 200,
            };
            invokeFunc.withArgs(sinon.match.any).returns({
                promise: () => invokeLambdaSuccessResult,
            });

            startExecutionFunc.withArgs(sinon.match.any).returns({
                promise: () => startExecutionResult,
            });
        });

        //Tests
        const event = require("./snsMessage.json");

        it("Should call inventoryMultiPart when size exceeds MAX_SIZE (default 4GB)", async () => {
            let mockMsg = JSON.parse(event.Records[0].Sns.Message);
            mockMsg.InventorySizeInBytes = MULTI_PART_SIZE;
            event.Records[0].Sns.Message = JSON.stringify(mockMsg);
            await expect(index.handler(event)).to.not.be.rejected;
        });
    });
});
