"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import pytest
import uuid
import boto3

from refreezer.infrastructure.output_keys import OutputKeys

if typing.TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table
else:
    DynamoDBClient = object
    Table = object


def test_table_access_pattern_and_partition_key() -> None:
    table_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
    client: DynamoDBClient = boto3.client("dynamodb")

    key = {"pk": {"S": "pk-testing"}, "sk": {"S": "sk-testing"}}
    value = str(uuid.uuid4())

    client.put_item(
        TableName=table_name, Item={**key, **{"testing_value": {"S": value}}}
    )
    assert (
        value
        == client.get_item(TableName=table_name, Key=key)["Item"]["testing_value"]["S"]
    )
    client.delete_item(TableName=table_name, Key=key)
