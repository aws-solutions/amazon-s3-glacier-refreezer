// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const { expect } = require("chai");
const sinon = require("sinon");
const proxyquire = require("proxyquire").noCallThru();

describe("-- Request Inventory Test --", () => {
    describe("-- Storage Service Test --", () => {
        var AWS;
        var storageService;

        var headBucketFunc;
        var putObjectFunc;
        var deleteObjectFunc;

        const testBucketName = "glacier-destination-bucket";

        //Init
        before(function () {
            headBucketFunc = sinon.stub();
            putObjectFunc = sinon.stub();
            deleteObjectFunc = sinon.stub();

            AWS = {
                S3: sinon.stub().returns({
                    headBucket: headBucketFunc,
                    putObject: putObjectFunc,
                    deleteObject: deleteObjectFunc,
                }),
            };

            const headBucketResult = { data: { status: 200 }, err: null };
            const putObjResult = {
                data: { ETag: "6805f2cfc46c0f04559748bb039d69ae", VersionId: "unyZwjQ69YxcQLA8z4F5j3kJJKr" },
                err: null,
            };
            const deleteObjResult = {
                data: { VersionId: "9_gKg5vG56F.TTEUdwkxGpJ3tNDlWlGq", DeleteMarker: true },
                err: null,
            };

            //Matchers
            headBucketFunc
                .withArgs(
                    sinon.match(function (param) {
                        return param.Bucket === testBucketName;
                    })
                )
                .returns({
                    promise: () => headBucketResult,
                });

            putObjectFunc
                .withArgs(
                    sinon.match(function (param) {
                        return param.Bucket === testBucketName;
                    })
                )
                .returns({
                    promise: () => putObjResult,
                });

            deleteObjectFunc
                .withArgs(
                    sinon.match(function (param) {
                        return param.Bucket === testBucketName;
                    })
                )
                .returns({
                    promise: () => deleteObjResult,
                });

            // Overwrite internal references with mock proxies
            storageService = proxyquire("../lib/storageService.js", {
                "aws-sdk": AWS,
            });
        });

        //Tests
        it("Should return TRUE if valid bucket name is supplied", async () => {
            const response = await storageService.checkBucketExists(testBucketName);
            expect(response).to.equal(true);
        });
        it("Should return FALSE if invalid bucket name is supplied", async () => {
            const response = await storageService.checkBucketExists("invalid-bucket");
            expect(response).to.equal(false);
        });
        it("Should return FALSE if bucket name is not supplied", async () => {
            const response = await storageService.checkBucketExists();
            expect(response).to.equal(false);
        });
    });
});
