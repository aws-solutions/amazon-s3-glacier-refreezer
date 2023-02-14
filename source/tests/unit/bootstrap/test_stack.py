"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import aws_cdk as core
import aws_cdk.assertions as assertions
import pytest

from refreezer.bootstrap.stack import BootstrapStack


@pytest.fixture
def template() -> assertions.Template:
    app = core.App()
    stack = BootstrapStack(app, "refreezer-bootstrap")
    template = assertions.Template.from_stack(stack)
    return template


def test_four_ssm_parameters_created(template: assertions.Template) -> None:
    template.resource_count_is("AWS::SSM::Parameter", 4)


def test_param_code_arn_created(template: assertions.Template) -> None:
    template.has_resource_properties(
        "AWS::SSM::Parameter",
        {
            "Description": "CodeStar Connection ARN",
            "Name": "/refreezer-build/connection/arn",
        },
    )


def test_param_repo_name_created(template: assertions.Template) -> None:
    template.has_resource_properties(
        "AWS::SSM::Parameter",
        {
            "Description": "GitHub repo name",
            "Name": "/refreezer-build/connection/repo",
        },
    )


def test_param_repo_branch_created(template: assertions.Template) -> None:
    template.has_resource_properties(
        "AWS::SSM::Parameter",
        {
            "Description": "GitHub repo branch",
            "Name": "/refreezer-build/connection/branch",
        },
    )


def test_param_repo_owner_created(template: assertions.Template) -> None:
    template.has_resource_properties(
        "AWS::SSM::Parameter",
        {
            "Description": "GitHub repo owner",
            "Name": "/refreezer-build/connection/owner",
        },
    )
