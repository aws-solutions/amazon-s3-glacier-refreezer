// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

// Imports
const fs = require('fs');

// Paths
const global_s3_assets = '../global-s3-assets';

// For each template in global_s3_assets ...
fs.readdirSync(global_s3_assets).forEach(file => {
    // Import and parse template file
    const raw_template = fs.readFileSync(`${global_s3_assets}/${file}`);
    let template = JSON.parse(raw_template);

    // Clean-up Lambda function code dependencies
    const resources = (template.Resources) ? template.Resources : {};
    const lambdaFunctions = Object.keys(resources).filter(function (key) {
        return resources[key].Type === 'AWS::Lambda::Function';
    });

    lambdaFunctions.forEach(function (f) {
        const fn = template.Resources[f];
        if (fn.Properties.Code.hasOwnProperty('S3Bucket')) {
            // Set the S3 key reference
            let artifactHash = fn.Properties.Code.S3Key;
            const assetPath = `asset${artifactHash}`;
            fn.Properties.Code.S3Key = `%%SOLUTION_NAME%%/%%VERSION%%/${assetPath}`;

            // Set the S3 bucket reference
            fn.Properties.Code.S3Bucket = {
                'Fn::Sub': '%%BUCKET_NAME%%-${AWS::Region}'
            };
        }
    });

    // Clean-up parameters section
    const parameters = (template.Parameters) ? template.Parameters : {};
    const assetParameters = Object.keys(parameters).filter(function (key) {
        return key.includes('AssetParameters');
    });
    assetParameters.forEach(function (a) {
        template.Parameters[a] = undefined;
    });

    // Cleaning up Standard and Expedited options for Glacier Retrieval Tiers
    // We want to make sure that customers do not incur high costs by accident
    template.Parameters.GlacierRetrievalTier.AllowedValues = [ "Bulk" ]

    // Output modified template file
    const output_template = JSON.stringify(template, null, 2);
    fs.writeFileSync(`${global_s3_assets}/${file}`, output_template);
});