"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
from typing import TYPE_CHECKING
import uuid
import boto3
from refreezer.infrastructure.stack import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_dynamodb.service_resource import Table
else:
    DynamoDBClient = object
    Table = object


def test_table_access_pattern_and_partition_key() -> None:
    table_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
    client: DynamoDBClient = boto3.client("dynamodb")

    items = []
    keys = []
    for i in range(9):
        value = str(uuid.uuid4())
        key = {"pk": {"S": "pk-testing"}, "sk": {"S": f"sk:{i}"}}
        item = {
            "pk": {"S": "pk-testing"},
            "sk": {"S": f"sk:{i}"},
            "testing_value": {"S": value},
        }
        items.append(item)
        keys.append(key)

    for item in items:
        client.put_item(TableName=table_name, Item=item)

    response = client.query(
        TableName=table_name,
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "pk-testing"}},
    )
    assert len(response["Items"]) == 9
    assert response["Items"][0] == items[0]
    assert response["Items"][8] == items[8]

    for key in keys:
        client.delete_item(TableName=table_name, Key=key)

    response = client.query(
        TableName=table_name,
        KeyConditionExpression="pk = :pk",
        ExpressionAttributeValues={":pk": {"S": "pk-testing"}},
    )
    assert len(response["Items"]) == 0
