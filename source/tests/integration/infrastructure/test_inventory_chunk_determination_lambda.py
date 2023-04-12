"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import boto3
import json

from refreezer.infrastructure.output_keys import OutputKeys

if typing.TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = object


def test_lambda_invoke() -> None:
    client: LambdaClient = boto3.client("lambda")
    lambda_name = os.environ[OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN]

    test_event = (
        '{"InventorySize": 100, "MaximumInventoryRecordSize": 2, "ChunkSize": 10}'
    )

    client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=test_event,
    )


def test_lambda_invoke_incomplete_parameters_request() -> None:
    client: LambdaClient = boto3.client("lambda")
    lambda_name = os.environ[OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN]

    test_event = '{"InventorySize": 100}'

    response = client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=test_event,
    )
    response_payload = response["Payload"]
    response_payload_data = json.loads(response_payload.read())
    assert "errorMessage" in response_payload_data
