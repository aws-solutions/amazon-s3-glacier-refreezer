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

const { expect, assert } = require('chai');
const sinon = require('sinon');
const proxyquire = require('proxyquire').noCallThru();


describe('-- Request Inventory Test --', () => {
    describe('-- Glacier Service Test --', () => {
        var AWS;
        var glacierService;
        var initiateJobFunc;

        const validJobName = 'TestSourceVault' + '_' + "request_inventory_job_" + Date.now();
        const retrievalType = "inventory-retrieval";

        //Init
        before(function () {
            initiateJobFunc = sinon.stub();

            AWS = {
                Glacier: sinon.stub().returns({
                    initiateJob: initiateJobFunc
                })
            }

            const successResult = {
                data: {
                    jobId: " HkF9p6o7yjhFx-K3CGl6fuSm6VzW9T7esGQfco8nUXVYwS0jlb5gq1JZ55yHgt5vP54ZShjoQzQVVh7vEXAMPLEjobID",
                    location: "/111122223333/vaults/examplevault/jobs/HkF9p6o7yjhFx-K3CGl6fuSm6VzW9T7esGQfco8nUXVYwS0jlb5gq1JZ55yHgt5vP54ZShjoQzQVVh7vEXAMPLEjobID"
                },
                err: null
            }
            const failedResult = {
                data: null,
                err: "Job initiation error"
            }

            // Matchers
            initiateJobFunc.withArgs(sinon.match(function (param) {
                return param.jobParameters.Description === validJobName && param.jobParameters.Type === retrievalType
            })).returns(
                {
                    promise: () => successResult
                }
            )
            initiateJobFunc.withArgs(sinon.match(function (param) {
                return param.jobParameters.Description !== validJobName
            })).returns(
                {
                    promise: () => failedResult
                }
            )

            // Overwrite internal references with mock proxies
            glacierService = proxyquire('../lib/glacierService.js', {
                'aws-sdk': AWS
            });

        })

        //Tests
        describe('startJob', () => {
            it('Should return RESPONSE if valid parameters passed', async () => {
                var params = {
                    accountId: "-",
                    jobParameters: {
                        Description: validJobName,
                        Format: "CSV",
                        SNSTopic: "TestsnsTopic",
                        Type: "inventory-retrieval"
                    },
                    vaultName: "TestSourceVault"
                };

                const response = await glacierService.startJob(params);
                expect(response.err).to.null;
                expect(response.data.location).to.not.null;
                expect(response.data.jobId).to.not.null;
            })

            it('Should return ERROR if valid parameters are not passed', async () => {
                var params = {
                    accountId: "-",
                    jobParameters: {
                        Description: "some-random-name",
                        Format: "CSV",
                        SNSTopic: "TestsnsTopic",
                        Type: "inventory-retrieval"
                    },
                    vaultName: "TestSourceVault"
                };

                const response = await glacierService.startJob(params);
                expect(response.err).to.not.null;
                expect(response.data).to.null;
                expect(response.data).to.null;
            })
        })

    })
})
