"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing

import boto3
import pytest

from refreezer.infrastructure.stack import OutputKeys
from refreezer.pipeline.stack import STACK_NAME

if typing.TYPE_CHECKING:
    from mypy_boto3_cloudformation import CloudFormationClient
else:
    CloudFormationClient = object


@pytest.fixture(autouse=True, scope="package")
def set_up_environment() -> None:
    client: CloudFormationClient = boto3.client("cloudformation")
    result = client.describe_stacks(StackName=STACK_NAME)["Stacks"]
    assert 1 == len(result)
    assert "Outputs" in result[0]
    for output in result[0]["Outputs"]:
        os.environ[output["OutputKey"]] = output["OutputValue"]
