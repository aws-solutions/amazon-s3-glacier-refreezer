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

'use strict';

const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

describe('-- Calculate Metrics --', () => {
    describe('-- Dynamo metric count--', () => {
        var AWS;
        var progressCount;
        var metricResult;
        var finalMetricCount = 32000;
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

            // Overwrite internal references with mock proxies
            dynamo = proxyquire('../lib/dynamo.js', {
                'aws-sdk': AWS
            })
        });

        describe('-- Dynamo --', () => {
            it('Should Dynamo return Processing Counts to expected metric values', async () => {
                var res = await dynamo.getItem('count');
                expect(res.total.N).to.be.equal(finalMetricCount);
                expect(res.started.N).to.be.equal(finalMetricCount);
                expect(res.requested.N).to.be.equal(finalMetricCount);
                expect(res.completed.N).to.be.equal(finalMetricCount);
                expect(res.validated.N).to.be.equal(finalMetricCount);
            });
        });
    });

    describe('-- Dynamo metric volume--', () => {
        var AWS;
        var progressVolume;
        var metricResult;
        var finalMetricVolume = 1000000;
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

            progressVolume = {
                Items: [{
                    "pk": "volume",
                    "total": {'N': finalMetricVolume},
                    "completed": {'N': finalMetricVolume},
                    "requested": {'N': finalMetricVolume},
                    "started": {'N': finalMetricVolume},
                    "validated": {'N': finalMetricVolume}
                }]
            };

            //Matchers
            queryFunc.withArgs(sinon.match(function (param) {
                return param.ExpressionAttributeValues[Object.keys(param.ExpressionAttributeValues)[0]]['S'] === 'volume'
            })).returns(
                {
                    promise: () => progressVolume
                }
            );

            // Overwrite internal references with mock proxies
            dynamo = proxyquire('../lib/dynamo.js', {
                'aws-sdk': AWS
            })
        });

        describe('-- Dynamo --', () => {

            it('Should Dynamo return Processing Volumes to expected metric values', async () => {
                var res = await dynamo.getItem('volume');
                expect(res.total.N).to.be.equal(finalMetricVolume);
                expect(res.started.N).to.be.equal(finalMetricVolume);
                expect(res.requested.N).to.be.equal(finalMetricVolume);
                expect(res.completed.N).to.be.equal(finalMetricVolume);
                expect(res.validated.N).to.be.equal(finalMetricVolume);
            });
        });
    });
});
