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

describe('-- Dowload Inventory Part Test --', () => {
    var AWS;
    var getJobOutputFunc;
    var uploadPartFunc;
    var index;

    const validJobId = "zbxcm3Z_3z5UkoroF7SuZKrxgGoDc3RloGduS7Eg-RO47Yc6FxsdGBgf_Q2DK5Ejh18CnTS5XW4_XqlNHS61dsO4CnMW"
    const startByte = 0;
    const endByte = 1023;
    //Init
    before(function () {
        getJobOutputFunc = sinon.stub();
        uploadPartFunc = sinon.stub();

        AWS = {
            Glacier: sinon.stub().returns({
                getJobOutput: getJobOutputFunc
            }),
            S3: sinon.stub().returns({
                uploadPart: uploadPartFunc
            })
        }

        //Matchers
        const jobOutPutStream = Buffer.alloc(endByte - startByte + 1);
        jobOutPutStream.write("Binary stream output retrieved from Glacier");
        const jobOutputSuccessResult = {
            data: {
                acceptRanges: "bytes",
                body: jobOutPutStream,
                contentType: "application/json",
                status: 200
            },
            err: null
        }
        getJobOutputFunc.withArgs(sinon.match(function (param) {
            return param.jobId === validJobId
        })).returns(
            {
                createReadStream: sinon.stub().returns(jobOutputSuccessResult)
            }
        )
        getJobOutputFunc.withArgs(sinon.match(function (param) {
            return param.jobId !== validJobId
        })).returns(
            {
                createReadStream: () => { throw new Error('Job not found') }
            }
        )

        const multiPartUplaodResult = {
            data: {
                ETag: "\"d8c2eafd90c266e19ab9dcacc479f8af\""
            },
            err: null

        }
        uploadPartFunc.withArgs(sinon.match.any).returns(
            {
                promise: () => multiPartUplaodResult
            }
        )

        // Overwrite internal references with mock proxies
        index = proxyquire('../index.js', {
            'aws-sdk': AWS
        })
    })


    //Tests
    describe('index', () => {
        it('Should return valid RESPONSE if valid parameter with job id is supplied', async () => {
            var mockEvent = {
                jobId: validJobId,
                startByte: startByte,
                endByte: endByte,
                vaultName: "testVault",
            }
            const response = await index.handler(mockEvent);
            expect(response.data).to.not.null;
            expect(response.err).to.null;
        })

        it('Should return ERROR if invalid parameter with job id is supplied', async () => {
            var mockEvent = {
                jobId: "some-random-job-id", //Invalid job id
                startByte: startByte,
                endByte: endByte,
                vaultName: "testVault",
            }
            await expect(index.handler(mockEvent)).to.be.rejectedWith(`Job not found`);
        })
    })
});