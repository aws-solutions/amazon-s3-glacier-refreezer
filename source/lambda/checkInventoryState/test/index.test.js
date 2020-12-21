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

process.env.INVENTORY_TABLE = 'inventory-table';
process.env.PARTITIONED_TABLE = 'partitioned-table';

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

sinon.stub(console, 'error');

describe('--  Get Count Query Result Test --', () => {
    let AWS;

    let index;

    let getQueryResultsFunc;
    let getQueryExecutionFunc;
    let startQueryExecutionFunc;
    let putItemFunc;
    let marshallFunc;
    let resultThree;
    let resultZero;
    let dynamoResult;

    //Init
    before(function () {
        getQueryResultsFunc = sinon.stub();
        getQueryExecutionFunc = sinon.stub();
        startQueryExecutionFunc = sinon.stub();
        putItemFunc = sinon.stub();
        marshallFunc = sinon.stub();



        AWS = {
            Athena: sinon.stub().returns({
                getQueryResults: getQueryResultsFunc,
                getQueryExecution: getQueryExecutionFunc,
                startQueryExecution: startQueryExecutionFunc,
            }),
            DynamoDB: sinon.stub().returns({
                putItem: putItemFunc,
            })
        }
        let Converter = {
            marshall: marshallFunc
        }
        AWS.DynamoDB.Converter = Converter


        dynamoResult = {
            ConsumedCapacity: {
                CapacityUnits: 1,
                TableName: "default-table"
            }
        }


        resultZero = {
            "ResultSet": {
                "Rows": [
                    { "Data": [{ "VarCharValue": "number" }] },
                    { "Data": [{ "VarCharValue": "0" }] }
                ]
            }
        }

        resultThree = {
            "ResultSet": {
                "Rows": [
                    { "Data": [{ "VarCharValue": "number" }] },
                    { "Data": [{ "VarCharValue": "3" }] }
                ]
            }
        }

        // [ TEST 1 ]inventory 3, partitioned : 3 ==> skipInit : true
        getQueryResultsFunc.onCall(0).returns({
            promise: () => resultThree

        })

        getQueryResultsFunc.onCall(1).returns({
            promise: () => resultThree
        })

        // [ TEST 2] inventory 0 ==> Exception
        getQueryResultsFunc.onCall(2).returns({
            promise: () => resultZero
        })

        getQueryResultsFunc.onCall(3).returns({
            promise: () => resultZero
        })

        // [ TEST 3 ] inventory 3, partitioned : 0 ==> skipInit : false
        getQueryResultsFunc.onCall(4).returns({
            promise: () => resultThree
        })

        getQueryResultsFunc.onCall(5).returns({
            promise: () => resultZero
        })
        startQueryExecutionFunc.withArgs(sinon.match.any).returns({
            promise: () => {
                return { "QueryExecutionId": "default-execution-id" }
            }
        })

        let executionStatus = {
            QueryExecution: {
                Status:
                    { State: "SUCCEEDED" }
            }
        }

        getQueryExecutionFunc.withArgs(sinon.match.any).returns({
            promise: () => executionStatus
        })

        putItemFunc.withArgs(sinon.match.any).returns({
            promise: () => dynamoResult
        })

        marshallFunc.withArgs(sinon.match.any).returns({
            promise: () => new Promise()
        })

        // Overwrite internal references with mock proxies
        index = proxyquire('../index.js', {
            'aws-sdk': AWS
        })
    })

    // [ TEST 1 ]
    it('Should RETURN Skip Init status to true', async () => {
        const response = await index.handler("");
        expect(response.skipInit).to.be.equal(true);
    }).timeout(20000)

    // [ TEST 2 ]
    it('Should THROW an exception - inventory is 0', async () => {
        try {
            await index.handler("");
        } catch (e) {
            expect(e).to.be.a('Error');
        }
    }).timeout(20000)

    // [ TEST 3 ]
    it('Should RETURN Skip Init status to false', async () => {
        const response = await index.handler("");
        expect(response.skipInit).to.be.equal(false);
    }).timeout(15000)

})