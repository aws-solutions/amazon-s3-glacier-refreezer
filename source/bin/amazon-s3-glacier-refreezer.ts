#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { AmazonS3GlacierRefreezerStack } from '../template/amazon-s3-glacier-refreezer-stack';

const app = new cdk.App();

new AmazonS3GlacierRefreezerStack(
    app,
    process.env.GRF_STACK_NAME ? process.env.GRF_STACK_NAME : 'amazon-s3-glacier-refreezer',
    {
        solutionId: 'SO0140',
        description: '(SO0140) - Amazon S3 Glacier Re:Freezer copies Amazon S3 Glacier Vault archives to Amazon S3 Bucket. Version %%VERSION%%',
        solutionName: 'amazon-s3-glacier-refreezer'
    }
);
