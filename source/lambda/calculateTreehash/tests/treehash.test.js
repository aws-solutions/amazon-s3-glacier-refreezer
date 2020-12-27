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

const fs = require('fs');
const chai = require('chai');
const chaiAsPromised = require('chai-as-promised');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

const ONE_MB = 1024 * 1024;

// (Optional) Keep test output free of error messages printed by our lambda function
// sinon.stub(console, 'error');

describe('-- Calculate TreeHash Test --', () => {
    describe('-- TreeHash Test --', () => {
        var AWS;
        var treehash;
        var getObjectFunc;
        var readStream;
        const fixtureFile = 'tests/fixtures/data.bin';

        //Init
        before(function () {
            getObjectFunc = sinon.stub();

            AWS = {
                S3: sinon.stub().returns({
                    getObject: getObjectFunc
                })
            };
            getObjectFunc.withArgs(sinon.match(function (param) {
                let [start, end] = param.Range.replace(/bytes=/, "").split("-");
                readStream = fs.createReadStream(fixtureFile, { start: parseInt(start), end: parseInt(end) });
                return true;
            })).returns({ createReadStream: () => { return readStream } });

            // Overwrite internal references with mock proxies
            treehash = proxyquire('../lib/treehash.js', {
                'aws-sdk': AWS
            });
        })

        //Tests
        describe('-- getChunkHash --', () => {
            it('Should RETURN valid SHA256 hash for the whole 2Mb fixture file', async () => {
                const chunkHash = 'c7a55f3b8f819232d326caffd03a9ac121b9b84bad058214bc6a30c8d853143d';
                const response = await treehash.getChunkHash('data-2mb.bin', '2mb', 0, 2 * ONE_MB);
                expect(response.length).to.be.equal(64);
                expect(response).to.be.equal(chunkHash);
            })
        })
    })
});
