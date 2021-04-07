import {expect as expectCDK, matchTemplate, MatchStyle, SynthUtils} from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import {AmazonS3GlacierRefreezerStack} from '../lib/amazon-s3-glacier-refreezer-stack';

let stack: cdk.Stack;

beforeAll(() => {
    const app = new cdk.App();
    stack = new AmazonS3GlacierRefreezerStack(app, 'test-stack', {
        solutionId: 'SO0140',
        solutionName: 'amazon-s3-glacier-refreezer',
        description: '(SO0140) - Amazon S3 Glacier Re:Freezer copies Amazon S3 Glacier Vault archives to Amazon S3 Bucket. Version %%VERSION%%'
    });
});

test('Stack Snapshot', () => {
    expect(SynthUtils.toCloudFormation(stack)).toMatchSnapshot();
});
