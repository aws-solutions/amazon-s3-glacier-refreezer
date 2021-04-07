/*********************************************************************************************************************
 *  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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
const proxyquire = require('proxyquire').noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

describe('-- Calculate Metrics --', () => {
    describe('-- Dynamo --', () => {
        var AWS;
        var progressCount;
        var metricResult;
        var finalMetricCount = 32000
        var dynamo;
        var queryFunc;

        //Init
        before(function () {
            queryFunc = sinon.stub();

            AWS = {
                DynamoDB: sinon.stub().returns({
                    query: queryFunc
                })
            };

            progressCount = {
                Items: [{
                    "pk": "count",
                    "total": {'N': finalMetricCount},
                    "completed": {'N': finalMetricCount},
                    "requested": {'N': finalMetricCount},
                    "started": {'N': finalMetricCount},
                    "validated": {'N': finalMetricCount}
                }]
            };

            //Matchers
            queryFunc.withArgs(sinon.match(function (param) {
                return param.ExpressionAttributeValues[Object.keys(param.ExpressionAttributeValues)[0]]['S'] === 'count'
            })).returns(
                {
                    promise: () => progressCount
                }
            );

            metricResult = {
                'Total Archives': 0,
                'Requested from Glacier': 0,
                'Copy Initiated': 0,
                'Copy Completed': 0,
                'Hashes Validated': 0
            };

            // Overwrite internal references with mock proxies
            dynamo = proxyquire('../lib/dynamo.js', {
                'aws-sdk': AWS
            })
        });

        describe('-- Dynamo --', () => {
            it('Should Dynamo return Processing Counts to expected metric values', async () => {
                var res = await dynamo.getCount();
                expect(res.total.N).to.be.equal(finalMetricCount);
                expect(res.started.N).to.be.equal(finalMetricCount);
                expect(res.requested.N).to.be.equal(finalMetricCount);
                expect(res.completed.N).to.be.equal(finalMetricCount);
                expect(res.validated.N).to.be.equal(finalMetricCount);
            });
        });
    });
});
