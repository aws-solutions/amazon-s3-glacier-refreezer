"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import boto3
import json

from refreezer.infrastructure.stack import OutputKeys

if typing.TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = object


def test_lambda_invoke() -> None:
    # ClientError exception will be thrown if the invocation of the Lambda function fails.

    client: LambdaClient = boto3.client("lambda")
    lambda_name = os.environ[OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN]

    test_event = ""

    client.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        Payload=test_event,
    )
