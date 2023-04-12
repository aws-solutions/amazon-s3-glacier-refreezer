"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import boto3
from refreezer.infrastructure.output_keys import OutputKeys

if typing.TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object


def test_state_machine_start_execution() -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.RETRIEVE_ARCHIVE_STATE_MACHINE_ARN]
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None
