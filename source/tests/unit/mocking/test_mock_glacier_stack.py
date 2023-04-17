"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING

import aws_cdk as core
import aws_cdk.assertions as assertions
import cdk_nag
import pytest

from refreezer.mocking.mock_glacier_stack import MockGlacierStack


@pytest.fixture
def stack() -> MockGlacierStack:
    app = core.App()
    stack = MockGlacierStack(app, "mockGlacierStack")
    core.Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks(log_ignores=True, verbose=True)
    )
    return stack


@pytest.fixture
def template(stack: MockGlacierStack) -> assertions.Template:
    return assertions.Template.from_stack(stack)


def test_mock_glacier_initiate_job_lambda_created(
    stack: MockGlacierStack, template: assertions.Template
) -> None:
    resources = template.find_resources(
        type="AWS::Lambda::Function",
        props={
            "Properties": {
                "Handler": "refreezer.application.mocking.handlers.mock_glacier_initiate_job_task_handler",
                "Runtime": "python3.9",
            },
        },
    )
    assert 1 == len(resources)
