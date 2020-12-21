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

describe('-- Calculate Metrics Test --', () => {
    var AWS;

    var index;
    var result;
    var updateItemFunc;

    var increment = 1;

    //Init
    before(function () {
        updateItemFunc = sinon.stub();

        AWS = {
            DynamoDB: sinon.stub().returns({
                updateItem: updateItemFunc
            })
        }

        //Matchers
        result = {
            Item: {
                "completed": 0,
                "pk": "processProgress",
                "requested": 0,
                "started": 0,
                "validated": 0
            }
        }
        updateItemFunc.withArgs(sinon.match.any).returns(
            {
                promise: () => {
                    result.Item.completed += increment;
                    result.Item.requested += increment;
                    result.Item.started += increment;
                    result.Item.validated += increment;
                    return result
                }
            }
        )

        // Overwrite internal references with mock proxies
        index = proxyquire('../index.js', {
            'aws-sdk': AWS
        })
    })

    //Tests
    it('Should increment records in Dynamo DB', async () => {
        const response = await index.incrementCount(increment, increment, increment, increment);
        expect(result.Item.completed).to.be.equal(increment);
        expect(result.Item.requested).to.be.equal(increment);
        expect(result.Item.started).to.be.equal(increment);
        expect(result.Item.validated).to.be.equal(increment);

    })
});