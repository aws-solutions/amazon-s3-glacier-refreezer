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


@pytest.fixture
def default_input() -> str:
    input = {
        "items": [
            {
                "item": [
                    {
                        "description": "test description 1",
                        "vault_name": "test-vault-01",
                        "archive_id": "zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg",
                        "tier": "Standard",
                    },
                    {
                        "description": "test description 2",
                        "vault_name": "test-vault-01",
                        "archive_id": "sdmftEGsdsfghy4rVi2sdWEGTARKqWrQBOJBsd_gdrZ_Mlr76GEahnjgSUYisad1565MhCgE76VRPMpdsfdsfieipbCWdDtrtr_qoiIGfxniYP5ebRmufghdfT7Y-tJ8DDFdwtrt2y",
                        "tier": "Standard",
                    },
                ]
            }
        ]
    }
    return json.dumps(input)


def test_state_machine_start_execution(default_input: str) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None
