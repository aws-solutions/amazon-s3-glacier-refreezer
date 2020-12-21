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

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinon = require('sinon');
const { CodePipeline } = require('aws-sdk');
const proxyquire = require('proxyquire').noCallThru();


const expect = chai.expect;
chai.use(chaiAsPromised);

// (Optional) Keep test output free of error messages printed by our lambda function
sinon.stub(console, 'error');

describe('-- Request Archives Test --', () => {
    var AWS;

    var partition;

    var queryFunc;
    var startQueryExecutionFunc;
    var getQueryExecutionFunc;
    var getObjectFunc;

    var validArchiveId = '-_27G6RJ0mYFtcF4dF9_eWRPYFkndowEpxodhax26-t9UXFI-AaEZszxf80pu_4JCPvOGMIUA933I80uqRX9eZBhQN8umpBt1GXZUNeUGJKgYGJwA41cwqz7hFe4W5FZQoBMEpEdQA';
    var originalDateVal = '2020-01-01T01:00:24+00:00';
    var exepectedFName = 'data01/Objectv40682';
    var expectedQueryExecutionId = 'zbxcm3Z_3z5UkoroF7SuZKrxgGoDc3RloGduS7Eg-RO47Yc6FxsdGBgf_Q2DK5Ejh18CnTS5XW4_XqlNHS61dsO4CnMW';
    var jobOutPutStream;
    //Init
    before(function () {
        queryFunc = sinon.stub();
        startQueryExecutionFunc = sinon.stub();
        getQueryExecutionFunc = sinon.stub();
        getObjectFunc = sinon.stub();


        AWS = {
            S3: sinon.stub().returns({
                getObject: getObjectFunc
            }),
            DynamoDB: sinon.stub().returns({
                query: queryFunc
            }),
            Glacier: sinon.stub().returns({
                startQueryExecution: startQueryExecutionFunc
            }),
            Athena: sinon.stub().returns({
                startQueryExecution: startQueryExecutionFunc,
                getQueryExecution: getQueryExecutionFunc
            }),
        }

    })

    describe('-- getPartitionMaxProcessedIfn --', () => {
        const expectedIFN = 2683;
        var statusIndexTableItems;
        //Init
        before(function () {
            //Matchers 
            statusIndexTableItems = {
                Items: [{
                    LastEvaluatedKey: {
                        "aid": { "S": validArchiveId },
                        "cc": { "N": 1 },
                        "cdt": "2020-08-23T10:18:22+00:00",
                        "cpdt": "2020-08-23T14:00:24+00:00",
                        "descr": "data01/Objectv40682",
                        "fname": { "S": exepectedFName },
                        "ifn": { 'N': expectedIFN },
                        "pid": 100,
                        "psdt": { 'S': originalDateVal },
                        "sha": "40debdebbb7e575f781c8d90d4fa78de1b7cc6723988644db8e4271ef863f079",
                        "sz": 1048576,
                        "vdt": { 'S': "2020-08-23T14:00:24+00:00" }
                    }
                }]
            }

            queryFunc.withArgs(sinon.match.any).returns({
                promise: () => statusIndexTableItems.Items[0]
            })

            // Overwrite internal references with mock proxies
            partition = proxyquire('../index.js', {
                'aws-sdk': AWS
            })
        })
        //Test
        it('Should RETURN max processed file number value from DynamoDB as integer', async () => {
            const payload = {
                currentPartition: 100,
                maxPartition: 1000
            }
            const response = await partition.getPartitionMaxProcessedFileNumber(payload.currentPartition);
            expect(response).to.be.equal(expectedIFN);
        })

    })

    describe('-- readAthenaPartition --', () => {
        var athenaQueryGetQueryExecResult;
        var athenaQueryStartExecResult;
        //Init
        before(function () {
            //Matchers 

            athenaQueryStartExecResult = {
                "QueryExecutionId": expectedQueryExecutionId
            }
            athenaQueryGetQueryExecResult = {
                "QueryExecution": {
                    "Query": "string",
                    "QueryExecutionContext": {
                        "Catalog": "string",
                        "Database": "string"
                    },
                    "QueryExecutionId": "string",
                    "ResultConfiguration": {
                        "EncryptionConfiguration": {
                            "EncryptionOption": "string",
                            "KmsKey": "string"
                        },
                        "OutputLocation": "string"
                    },
                    "StatementType": "string",
                    "Statistics": {
                        "DataManifestLocation": "string",
                        "DataScannedInBytes": "number",
                        "EngineExecutionTimeInMillis": "number",
                        "QueryPlanningTimeInMillis": "number",
                        "QueryQueueTimeInMillis": "number",
                        "ServiceProcessingTimeInMillis": "number",
                        "TotalExecutionTimeInMillis": "number"
                    },
                    "Status": {
                        "CompletionDateTime": Date.now.toString(),
                        "State": "SUCCEEDED",
                        "StateChangeReason": "string",
                        "SubmissionDateTime": "number"
                    },
                    "WorkGroup": "string"
                }
            }

            startQueryExecutionFunc.withArgs(sinon.match.any).returns({
                promise: () => athenaQueryStartExecResult
            })
            getQueryExecutionFunc.withArgs(sinon.match.any).returns({
                promise: () => athenaQueryGetQueryExecResult
            })
        })
        //Test
        it('Should RETURN query execution id  from Athena', async () => {
            const response = await partition.readAthenaPartition(1);
            expect(response).to.be.equal(`results/${expectedQueryExecutionId}.csv`);
        })

    })
})