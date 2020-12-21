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
const proxyquire = require('proxyquire').noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

// (Optional) Keep test output free of error messages printed by our lambda function
sinon.stub(console, 'error');

describe('-- Collect Metrics Test --', () => {
    var AWS;

    var pubMetric;

    var archivesResult;
    var progressResult;
    var metricResult;
    var finalMetricCount = 32000

    var queryFunc;
    var putMetricDataFunc;

    //Init
    before(function () {
        queryFunc = sinon.stub();
        putMetricDataFunc = sinon.stub();

        AWS = {
            DynamoDB: sinon.stub().returns({
                query: queryFunc
            }),
            CloudWatch: sinon.stub().returns({
                putMetricData: putMetricDataFunc
            })
        }
        archivesResult = {
            Items: [{
                "pk": "totalRecordCount",
                "value": { 'N': finalMetricCount }
            }]
        }
        progressResult = {
            Items: [{
                "completed": { 'N': finalMetricCount },
                "pk": "processProgress",
                "requested": { 'N': finalMetricCount },
                "started": { 'N': finalMetricCount },
                "validated": { 'N': finalMetricCount }
            }]
        }

        //Matchers
        queryFunc.withArgs(sinon.match(function (param) {
            return param.ExpressionAttributeValues[Object.keys(param.ExpressionAttributeValues)[0]]['S'] === 'processProgress'
        })).returns(
            {
                promise: () => progressResult
            }
        )
        queryFunc.withArgs(sinon.match(function (param) {
            return param.ExpressionAttributeValues[Object.keys(param.ExpressionAttributeValues)[0]]['S'] === 'totalRecordCount'
        })).returns(
            {
                promise: () => archivesResult
            }
        )

        metricResult = {
            'Total Archives': 0,
            'Requested from Glacier': 0,
            'Copy Initiated': 0,
            'Copy Completed': 0,
            'Hashes Validated': 0
        }
        putMetricDataFunc.withArgs(sinon.match(function (param) {
            metricResult[param.MetricData[0].MetricName] = param.MetricData[0].Value
            return true;
        })).returns({
            promise: () => true
        })

        // Overwrite internal references with mock proxies
        pubMetric = proxyquire('../lib/publishMetric.js', {
            'aws-sdk': AWS
        })
    })

    //Tests
    it('Should Dynamo return Total Records to expected metric value', async () => {
        var res = await pubMetric.getTotalRecords({});
        expect(res).to.be.equal(finalMetricCount);
    })
    it('Should Dynamo return Total Processing Records  to expected metric values', async () => {
        var res = await pubMetric.getProcessProgress({});
        expect(res.started.N).to.be.equal(finalMetricCount);
        expect(res.requested.N).to.be.equal(finalMetricCount);
        expect(res.completed.N).to.be.equal(finalMetricCount);
        expect(res.validated.N).to.be.equal(finalMetricCount);
    })
    it('Should publish metric to CloudWatch', async () => {
        await expect(pubMetric.publishMetric({})).to.be.not.rejected;
    })
})