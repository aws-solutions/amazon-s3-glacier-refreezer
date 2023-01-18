"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import aws_cdk as core
import aws_cdk.assertions as assertions

from refreezer.infrastructure.stack import RefreezerStack


def test_ssm_parameter_created() -> None:
    app = core.App()
    stack = RefreezerStack(app, "refreezer")
    template = assertions.Template.from_stack(stack)
    template.resource_count_is("AWS::SSM::Parameter", 1)
    template.has_resource_properties(
        "AWS::SSM::Parameter", {"Name": "/refreezer/parameter"}
    )
