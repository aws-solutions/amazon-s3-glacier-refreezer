// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

const chai = require("chai");
const chaiAsPromised = require("chai-as-promised");
const sinon = require("sinon");
const proxyquire = require("proxyquire").noCallThru();

const expect = chai.expect;
chai.use(chaiAsPromised);

// (Optional) Keep test output free of error messages printed by our lambda function
sinon.stub(console, "error");

describe("-- Calculate Metrics Test --", () => {
    var AWS;

    var dynamo;
    var result;
    var updateItemFunc;

    var increment = 1;

    //Init
    before(function () {
        updateItemFunc = sinon.stub();

        AWS = {
            DynamoDB: sinon.stub().returns({
                updateItem: updateItemFunc,
            }),
        };

        //Matchers
        result = {
            Item: {
                copied: 0,
                pk: "count",
                requested: 0,
                staged: 0,
                validated: 0,
            },
        };
        updateItemFunc.withArgs(sinon.match.any).returns({
            promise: () => {
                result.Item.requested += increment;
                result.Item.staged += increment;
                result.Item.validated += increment;
                result.Item.copied += increment;
                return result;
            },
        });

        // Overwrite internal references with mock proxies
        dynamo = proxyquire("../lib/dynamo.js", {
            "aws-sdk": AWS,
        });
    });

    //Tests
    it("Should increment records in Dynamo DB", async () => {
        const response = await dynamo.incrementCount(increment, increment, increment, increment);
        expect(result.Item.copied).to.be.equal(increment);
        expect(result.Item.requested).to.be.equal(increment);
        expect(result.Item.staged).to.be.equal(increment);
        expect(result.Item.validated).to.be.equal(increment);
    });
});
