import { Template } from "aws-cdk-lib/assertions";

import { Stack, App, CustomResource, Duration, Aws } from "aws-cdk-lib";
import { AmazonS3GlacierRefreezerStack } from "../lib/amazon-s3-glacier-refreezer-stack";

let stack: Stack;
let template: Template;

beforeAll(() => {
    const app = new App();
    stack = new AmazonS3GlacierRefreezerStack(app, "test-stack", {
        solutionId: "SO0140",
        solutionName: "amazon-s3-glacier-refreezer",
        description:
            "(SO0140) - Amazon S3 Glacier Re:Freezer copies Amazon S3 Glacier Vault archives to Amazon S3 Bucket. Version %%VERSION%%",
    });
    template = Template.fromStack(stack);
});

test("Stack Snapshot", () => {
    expect(template).toMatchSnapshot();
});
