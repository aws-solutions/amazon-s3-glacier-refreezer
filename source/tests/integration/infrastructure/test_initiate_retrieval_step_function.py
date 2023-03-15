"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import boto3
import json
from refreezer.infrastructure.stack import OutputKeys
import pytest

if typing.TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object


def test_state_machine_start_execution() -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN]
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None
