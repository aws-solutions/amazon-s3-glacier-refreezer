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
    from mypy_boto3_stepfunctions import SFNClient
else:
    CloudFormationClient = object
    SFNClient = object


@pytest.fixture(autouse=True)
def set_up_environment() -> None:
    if OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN not in os.environ:
        client: CloudFormationClient = boto3.client("cloudformation")
        result = client.describe_stacks(StackName=STACK_NAME)["Stacks"]
        assert 1 == len(result)
        assert "Outputs" in result[0]
        for output in result[0]["Outputs"]:
            os.environ[output["OutputKey"]] = output["OutputValue"]


def test_state_machine_describe() -> None:
    state_machine_arn = os.environ[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN]
    client: SFNClient = boto3.client("stepfunctions")

    response = client.describe_state_machine(stateMachineArn=state_machine_arn)
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert "ACTIVE" == response["status"]
    assert "STANDARD" == response["type"]


def test_state_machine_start_execution() -> None:
    state_machine_arn = os.environ[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN]
    client: SFNClient = boto3.client("stepfunctions")

    response = client.start_execution(
        stateMachineArn=state_machine_arn,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None
