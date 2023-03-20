"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import typing
import pytest
import uuid
import boto3

from refreezer.infrastructure.stack import OutputKeys

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_sns import SNSClient
    from mypy_boto3_dynamodb.service_resource import Table
else:
    DynamoDBClient = object
    SNSClient = object
    Table = object


@pytest.fixture
def ddb_table(glacier_job_result: typing.Dict[str, typing.Any]) -> Table:
    ddb = boto3.resource("dynamodb")
    table_name = os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME]
    table: Table = ddb.Table(table_name)
    table.put_item(Item={"job_id": glacier_job_result["JobId"]})
    return table


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


def test_lambda_invoked() -> None:
    lambda_name = os.environ[OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME]
    client = boto3.client("lambda")
    response = client.invoke(FunctionName=lambda_name)
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]


def test_lambda_invoked_by_sns(
    glacier_job_result: typing.Dict[str, typing.Any], ddb_table: Table
) -> None:
    topic_arn = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]
    sns_client: SNSClient = boto3.client("sns")
    response = sns_client.publish(
        Message=json.dumps(glacier_job_result), TopicArn=topic_arn
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    lambda_name = os.environ[OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME]
    lambda_client = boto3.client("lambda")
    lambda_arn = lambda_client.get_function(FunctionName=lambda_name)["Configuration"][
        "FunctionArn"
    ]
    endpoint = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)[
        "Subscriptions"
    ][0]["Endpoint"]
    assert endpoint == lambda_arn
    table_item = ddb_table.get_item(Key={"job_id": glacier_job_result["JobId"]})
    assert table_item["Item"]

    post_lambda_cleanup(glacier_job_result["JobId"])


def post_lambda_cleanup(job_id: str) -> None:
    ddb = boto3.resource("dynamodb")
    table_name = os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME]
    table = ddb.Table(table_name)
    table.delete_item(Key={"job_id": job_id})
