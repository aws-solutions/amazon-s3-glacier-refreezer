"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import aws_cdk as core
import aws_cdk.assertions as assertions

from refreezer.pipeline.stack import PipelineStack


def test_pipeline_is_created() -> None:
    app = core.App()
    stack = PipelineStack(app, "pipeline")
    template = assertions.Template.from_stack(stack)
    template.resource_count_is("AWS::CodePipeline::Pipeline", 1)
