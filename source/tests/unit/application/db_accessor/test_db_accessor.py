"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import pytest
import moto  # type: ignore
import boto3
import logging


from mypy_boto3_dynamodb.service_resource import Table

from typing import Generator, TYPE_CHECKING, Dict
from refreezer.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_dynamodb.service_resource import Table
else:
    DynamoDBServiceResource = object
    Table = object


@pytest.fixture()
def dynamodb_accessor_mock() -> DynamoDBAccessor:
    return DynamoDBAccessor(os.getenv("DDB_TABLE_NAME", ""))


@pytest.fixture()
def ddb_table_item() -> Dict[str, str]:
    return {"job_id": "123", "task_token": "xadsd", "start_timestamp": "11:11:11"}


def test_insert_item(
    common_dynamodb_table_mock: Table,
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, str],
) -> None:
    dynamodb_accessor_mock.insert_item(ddb_table_item)
    response = common_dynamodb_table_mock.get_item(Key=ddb_table_item)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_get_item(
    common_dynamodb_table_mock: Table,
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, str],
) -> None:
    common_dynamodb_table_mock.put_item(Item=ddb_table_item)
    assert dynamodb_accessor_mock.get_item(ddb_table_item) == ddb_table_item


def test_get_item_not_found(
    common_dynamodb_table_mock: DynamoDBServiceResource,
    dynamodb_accessor_mock: DynamoDBAccessor,
) -> None:
    assert (
        dynamodb_accessor_mock.get_item(
            {"job_id": "4123", "task_token": "xadsd", "start_timestamp": "12:11:11"}
        )
        is None
    )


def test_update_item(
    common_dynamodb_table_mock: Table,
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    common_dynamodb_table_mock.put_item(Item=ddb_table_item)
    update_expression = "set forth_column = :somthing"
    expression_attribute_values = {":somthing": {"S": "Jane Doe"}}
    with caplog.at_level(logging.INFO):
        dynamodb_accessor_mock.update_item(
            ddb_table_item, update_expression, expression_attribute_values
        )
        assert "Successfully updated the database" in caplog.text


def test_delete_item(
    common_dynamodb_table_mock: Table,
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    common_dynamodb_table_mock.put_item(Item=ddb_table_item)
    with caplog.at_level(logging.INFO):
        dynamodb_accessor_mock.delete_item(ddb_table_item)
        assert "Successfully deleted item from the database" in caplog.text


def test_query_items(
    common_dynamodb_table_mock: Table,
    dynamodb_accessor_mock: DynamoDBAccessor,
    ddb_table_item: Dict[str, str],
) -> None:
    common_dynamodb_table_mock.put_item(Item=ddb_table_item)
    key_condition_expression = "123"
    expected_items = [ddb_table_item]
    result = dynamodb_accessor_mock.query_items("job_id", key_condition_expression)
    assert result == expected_items
