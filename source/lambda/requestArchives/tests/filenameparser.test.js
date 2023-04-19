/*********************************************************************************************************************
 *  Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                      *
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

"use strict";
const chai = require("chai");
const expect = chai.expect;
const { parseFileName, detectAndParseDescription } = require("../lib/filenameparser.js");

describe("-- Request Archives Test --", () => {
    describe("-- Request Archives : File name parser --", () => {
        it("Should parse a quoted FastGlacier v1 format", function () {
            expect(
                detectAndParseDescription(
                    '"<ArchiveMetadata>           <Path>       UkVOREVSRkFSTTUvQjovb25lL3R3by90aHJlZS9jaDAxX3NlcTAwNF9zaDAwMTAuMjA0NC5kcHg=   </Path></ArchiveMetadata>"'
                )
            ).to.be.equal("RENDERFARM5/B:/one/two/three/ch01_seq004_sh0010.2044.dpx");
        });

        it("Should parse an unquoted FastGlacier v1 format", function () {
            expect(
                detectAndParseDescription(
                    "<ArchiveMetadata>           <Path>       UkVOREVSRkFSTTUvQjovb25lL3R3by90aHJlZS9jaDAxX3NlcTAwNF9zaDAwMTAuMjA0NC5kcHg=   </Path></ArchiveMetadata>"
                )
            ).to.be.equal("RENDERFARM5/B:/one/two/three/ch01_seq004_sh0010.2044.dpx");
        });

        it("Should parse FastGlacier v2/3/4 format", function () {
            expect(
                detectAndParseDescription(
                    '"       <m><v>4</v><p>RlgxNTAvQ0JCX2ZpZ3VyZS9lZTRjM2MxOS00OTBkLTQwN2UtOGJlNy04ODliZTQxYjQ4YmIuY2Ji </p><lm>20190121T085242Z       </lm>        </m>"'
                )
            ).to.be.equal("FX150/CBB_figure/ee4c3c19-490d-407e-8be7-889be41b48bb.cbb");
        });

        it("Should parse CloudBerry JSON format", function () {
            expect(
                detectAndParseDescription(
                    '{"Path":"CB+F8-M5/B:/c01/q004/s0010/c01+AF8-s00+A8-s0010+F8/h01.1094.dpx","UTCDateModified":"20100310T110623Z"}'
                )
            ).to.be.equal("CB+F8-M5/B:/c01/q004/s0010/c01+AF8-s00+A8-s0010+F8/h01.1094.dpx");
        });

        it("Should parse empty file name from JSON", function () {
            expect(parseFileName("AAAQQQ", '{"Path":"   ","UTCDateModified":"20100310T110623Z"}')).to.be.equal(
                "00undefined/AAAQQQ"
            );
        });

        it("Should parse empty archive description", function () {
            expect(parseFileName("AAAQQQ", "             ")).to.be.equal("00undefined/AAAQQQ");
        });

        it("Should parse backward slashes as forward", function () {
            expect(parseFileName("AAAQQQ", "aaa\\bbb\\ccc")).to.be.equal("aaa/bbb/ccc");
        });
    });
});
