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

describe('-- Calculate TreeHash Test --', () => {
    describe('-- Update Storage Class Test --', () => {
        var AWS;

        var listObjectsV2Func;
        var copyObjectFunc;

        var storageClass;

        const validBucketName = 'Test-Glacier-Bucket';
        const desiredStorageClass = 'DEEP_ARCHIVE'
        var s3Results;
        //Init
        before(function () {
            process.env.STAGING_BUCKET = validBucketName;
            process.env.STAGING_BUCKET_PREFIX = "stagingdata"
            process.env.STORAGE_CLASS = desiredStorageClass;

            listObjectsV2Func = sinon.stub();
            copyObjectFunc = sinon.stub();

            AWS = {
                S3: sinon.stub().returns({
                    listObjectsV2: listObjectsV2Func,
                    copyObject: copyObjectFunc
                })
            }
            //Matchers
            s3Results = {
                "Name": validBucketName,
                "Prefix": null,
                "KeyCount": "205",
                "MaxKeys": "1000",
                "IsTruncated": "false",
                "Contents": [
                    {
                        "Key": "test-file.jpg",
                        "LastModified": "2009-10-12T17:50:30.000Z",
                        "ETag": "\"fba9dede5f27731c9771645a39863328\"",
                        "Size": "434234",
                        "StorageClass": "STANDARD"
                    }
                ]
            }
            listObjectsV2Func.withArgs(sinon.match(function (param) {
                return param.Prefix === 'stagingdata/non-standard-key';
            })).returns(
                {
                    promise: () => { throw new Error(`The source object is not Standard. Skipping updating Storage Class to ${desiredStorageClass}`) }
                }
            )
            listObjectsV2Func.withArgs(sinon.match(function (param) {
                return param.Prefix === 'stagingdata/standard-key'
            })).returns(
                {
                    promise: () => s3Results
                }
            )
            copyObjectFunc.withArgs(sinon.match(function (param) {
                s3Results.Contents[0].StorageClass = desiredStorageClass;
                return true;
            })).returns(
                {
                    promise: () => true
                }
            )
            // Overwrite internal references with mock proxies
            storageClass = proxyquire('../lib/copy.js', {
                'aws-sdk': AWS
            })
        })
        afterEach(() => {
            delete process.env.STAGING_BUCKET;
            delete process.env.STAGING_BUCKET_PREFIX;
            delete process.env.STORAGE_CLASS;
        });
        //Tests
        it('Should not DO ANYTHING if source object storage class is not STANDARD', async () => {
            await expect(storageClass.copyKeyToDestinationBucket('non-standard-key', 1024)).to.be.rejectedWith('The source object is not Standard. Skipping updating Storage Class to DEEP_ARCHIVE');
        })
        it('Should UPDATE/MOVE  object to desired storage class', async () => {
            const response = await storageClass.copyKeyToDestinationBucket('standard-key', 1024);
            console.log(`vfjfvefjv ${response}`)
            expect(s3Results.Contents[0].StorageClass).to.be.equal(desiredStorageClass);
        })
    })
})
