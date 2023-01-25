"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import uuid

import boto3
import pytest

from refreezer.infrastructure.stack import OutputKeys
from refreezer.pipeline.stack import STACK_NAME

if typing.TYPE_CHECKING:
    from mypy_boto3_cloudformation import CloudFormationClient
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_sns import SNSClient
else:
    CloudFormationClient = object
    DynamoDBClient = object
    SNSClient = object


@pytest.fixture(autouse=True)
def set_up_environment() -> None:
    if OutputKeys.ASYNC_FACILITATOR_TABLE_NAME not in os.environ:
        client: CloudFormationClient = boto3.client("cloudformation")
        result = client.describe_stacks(StackName=STACK_NAME)["Stacks"]
        assert 1 == len(result)
        assert "Outputs" in result[0]
        for output in result[0]["Outputs"]:
            os.environ[output["OutputKey"]] = output["OutputValue"]


def test_table_access_pattern_and_partition_key() -> None:
    table_name = os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME]
    client: DynamoDBClient = boto3.client("dynamodb")

    key = {"job_id": {"S": "testing"}}
    value = str(uuid.uuid4())

    client.put_item(
        TableName=table_name, Item={**key, **{"testing_value": {"S": value}}}
    )
    assert (
        value
        == client.get_item(TableName=table_name, Key=key)["Item"]["testing_value"]["S"]
    )
    client.delete_item(TableName=table_name, Key=key)


def test_topic_publish() -> None:
    topic_arn = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]
    client: SNSClient = boto3.client("sns")

    response = client.publish(Message="test message", TopicArn=topic_arn)
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
