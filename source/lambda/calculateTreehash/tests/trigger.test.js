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

describe('-- Trigger Message to copyToDestinationQueue Test --', () => {
    describe('-- trigger Test --', () => {
        var AWS;

        var trigger;

        var getQueueUrlFunc;
        var sendMessageFunc;

        var queueUrlResult;
        var sendMessageResult;
        var singleDBresult;
        var validArchiveId = '-_27G6RJ0mYFtcF4dF9_eWRPYFkndowEpxodhax26-t9UXFI-AaEZszxf80pu_4JCPvOGMIUA933I80uqRX9eZBhQN8umpBt1GXZUNeUGJKgYGJwA41cwqz7hFe4W5FZQoBMEpEdQA';
        var originalDateVal = '2020-01-01T01:00:24+00:00';
        var exepectedFName = 'data01/Objectv40682';
        //Init
        before(function () {
            getQueueUrlFunc = sinon.stub();
            sendMessageFunc = sinon.stub();


            AWS = {
                SQS: sinon.stub().returns({
                    getQueueUrl: getQueueUrlFunc,
                    sendMessage: sendMessageFunc
                })
            }

            //Matchers
            queueUrlResult = {
                QueueUrl: 'https://sqs.ap-southeast-2.amazonaws.com/XXXXXXX/glacier-stack-XXX--copyToDestinationBucketQueue'
            }
            sendMessageResult = {
                "MD5OfMessageBody": "51b0a325...39163aa0",
                "MD5OfMessageAttributes": "00484c68...59e48f06",
                "MessageId": "da68f62c-0c07-4bee-bf5f-7e856EXAMPLE"
            }
            singleDBresult = {
                Attributes: {
                    "fname": {
                        S: "test-key-name"
                    },
                    "aid": {
                        S: "archive-id"
                    }
                }
            }
            getQueueUrlFunc.withArgs(sinon.match.any).returns(
                {
                    promise: () => queueUrlResult
                }
            )
            sendMessageFunc.withArgs(sinon.match.any).returns(
                {
                    promise: () => sendMessageResult
                }
            )
            // Overwrite internal references with mock proxies
            trigger = proxyquire('../lib/trigger.js', {
                'aws-sdk': AWS
            })
        })

        //Test
        it('should successfully send the messages through SQS', async () => {
            await expect(trigger.triggerCopyToDestinationBucket(singleDBresult)).to.be.not.rejected; //TODO: Original method doesnt return, perhaps await issue           
        })
    })
})