// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
const sinon = require("sinon");
const { CodePipeline } = require("aws-sdk");
const proxyquire = require("proxyquire").noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

// (Optional) Keep test output free of error messages printed by our lambda function
sinon.stub(console, "error");

describe("-- Request Archives Test --", () => {
    var AWS;

    var partition;

    var queryFunc;
    var startQueryExecutionFunc;
    var getQueryExecutionFunc;
    var getObjectFunc;

    var expectedQueryExecutionId =
        "zbxcm3Z_3z5UkoroF7SuZKrxgGoDc3RloGduS7Eg-RO47Yc6FxsdGBgf_Q2DK5Ejh18CnTS5XW4_XqlNHS61dsO4CnMW";

    //Init
    before(function () {
        queryFunc = sinon.stub();
        startQueryExecutionFunc = sinon.stub();
        getQueryExecutionFunc = sinon.stub();
        getObjectFunc = sinon.stub();

        AWS = {
            S3: sinon.stub().returns({
                getObject: getObjectFunc,
            }),
            DynamoDB: sinon.stub().returns({
                query: queryFunc,
            }),
            Glacier: sinon.stub().returns({
                startQueryExecution: startQueryExecutionFunc,
            }),
            Athena: sinon.stub().returns({
                startQueryExecution: startQueryExecutionFunc,
                getQueryExecution: getQueryExecutionFunc,
            }),
        };
    });

    describe("-- readAthenaPartition --", () => {
        var athenaQueryGetQueryExecResult;
        var athenaQueryStartExecResult;
        //Init
        before(function () {
            //Matchers

            athenaQueryStartExecResult = {
                QueryExecutionId: expectedQueryExecutionId,
            };
            athenaQueryGetQueryExecResult = {
                QueryExecution: {
                    Query: "string",
                    QueryExecutionContext: {
                        Catalog: "string",
                        Database: "string",
                    },
                    QueryExecutionId: "string",
                    ResultConfiguration: {
                        EncryptionConfiguration: {
                            EncryptionOption: "string",
                            KmsKey: "string",
                        },
                        OutputLocation: "string",
                    },
                    StatementType: "string",
                    Statistics: {
                        DataManifestLocation: "string",
                        DataScannedInBytes: "number",
                        EngineExecutionTimeInMillis: "number",
                        QueryPlanningTimeInMillis: "number",
                        QueryQueueTimeInMillis: "number",
                        ServiceProcessingTimeInMillis: "number",
                        TotalExecutionTimeInMillis: "number",
                    },
                    Status: {
                        CompletionDateTime: Date.now.toString(),
                        State: "SUCCEEDED",
                        StateChangeReason: "string",
                        SubmissionDateTime: "number",
                    },
                    WorkGroup: "string",
                },
            };

            startQueryExecutionFunc.withArgs(sinon.match.any).returns({
                promise: () => athenaQueryStartExecResult,
            });
            getQueryExecutionFunc.withArgs(sinon.match.any).returns({
                promise: () => athenaQueryGetQueryExecResult,
            });

            // Overwrite internal references with mock proxies
            partition = proxyquire("../index.js", {
                "aws-sdk": AWS,
            });
        });
        //Test
        it("Should RETURN query execution id  from Athena", async () => {
            const response = await partition.readAthenaPartition(1);
            expect(response).to.be.equal(`results/${expectedQueryExecutionId}.csv`);
        });
    });
});
