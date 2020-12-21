import { expect as expectCDK, matchTemplate, MatchStyle } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import * as AmazonS3GlacierRefreezer from '../template/amazon-s3-glacier-refreezer-stack';

test('Empty Stack', () => {
    const app = new cdk.App();
    // WHEN
    const stack = new AmazonS3GlacierRefreezer.AmazonS3GlacierRefreezerStack(app, 'test-stack',{solutionId: 'SO0140'});
    // THEN
    // expectCDK(stack).to(matchTemplate({ "Resources": {} }, MatchStyle.EXACT))
});
