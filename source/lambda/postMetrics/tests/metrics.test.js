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
    describe('-- Post Metrics Test --', () => {
        var AWS;
        var pubMetric;
        var metricResult;
        var putMetricDataFunc;

        //Init
        before(function () {
            putMetricDataFunc = sinon.stub();

            AWS = {
                CloudWatch: sinon.stub().returns({
                    putMetricData: putMetricDataFunc
                })
            };

            metricResult = {
                'Total Archives': 0,
                'Requested from Glacier': 0,
                'Copy Initiated': 0,
                'Copy Completed': 0,
                'Hashes Validated': 0
            };

            putMetricDataFunc.withArgs(sinon.match(function (param) {
                metricResult[param.MetricData[0].MetricName] = param.MetricData[0].Value
                return true;
            })).returns({
                promise: () => true
            });

            // Overwrite internal references with mock proxies
            pubMetric = proxyquire('../lib/metrics.js', {
                'aws-sdk': AWS
            });
        });

        describe('-- Post Metrics Test --', () => {
            it('Should publish metric to CloudWatch', async () => {
                await expect(pubMetric.publishMetric()).to.be.not.rejected;
            });
        });
    });
});
