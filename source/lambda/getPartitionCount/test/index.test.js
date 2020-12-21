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

describe('--  Get Partition Count Test --', () => {
    let AWS;

    let index;

    let getQueryResultsFunc;
    let getQueryExecutionFunc;
    let startQueryExecutionFunc;

    //Init
    before(function () {
        getQueryResultsFunc = sinon.stub();
        getQueryExecutionFunc = sinon.stub();
        startQueryExecutionFunc = sinon.stub();


        AWS = {
            Athena: sinon.stub().returns({
                getQueryResults: getQueryResultsFunc,
                getQueryExecution: getQueryExecutionFunc,
                startQueryExecution: startQueryExecutionFunc,
            })
        }

        const executionStatus = {
            QueryExecution: {
                Status:
                    { State: "SUCCEEDED" }
            }
        }
        const athenaResult = {
            "ResultSet": {
                "Rows": [
                    { "Data": [{ "VarCharValue": "number" },{ "VarCharValue": "number" }] },
                    { "Data": [{ "VarCharValue": "1" },{ "VarCharValue": "3" }] }

                ]
            }
        }
        startQueryExecutionFunc.withArgs(sinon.match.any).returns({
            promise: () => {
                return { "QueryExecutionId": "default-execution-id" }
            }
        })
        getQueryExecutionFunc.withArgs(sinon.match.any).returns({
            promise: () => executionStatus
        })
        getQueryResultsFunc.onCall(0).returns({
            promise: () => athenaResult
        })


        // Overwrite internal references with mock proxies
        index = proxyquire('../index.js', {
            'aws-sdk': AWS
        })
    })
    it('Should RETURN valid current and max partitions', async () => {
        const response = await index.handler("");        
        expect(response.currentPartition).to.be.equal(1);
        expect(response.maxPartition).to.be.equal(3);
        expect(response.isComplete).to.be.equal(false);
    })

})