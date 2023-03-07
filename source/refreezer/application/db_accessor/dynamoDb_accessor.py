"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import boto3
import logging
from typing import TYPE_CHECKING, Optional, Dict, List, Any

from boto3.dynamodb.conditions import Key


if TYPE_CHECKING:
    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_dynamodb.service_resource import Table
else:
    DynamoDBServiceResource = object
    Table = object

logger = logging.getLogger()


class DynamoDBAccessor:
    def __init__(self, table_name: str) -> None:
        self.dynamodb: DynamoDBServiceResource = boto3.resource("dynamodb")
        self.table: Table = self.dynamodb.Table(table_name)

    def insert_item(self, item: Dict[str, Any]) -> None:
        self.table.put_item(Item=item)
        logger.info("Successfully inserted item into the database")

    def get_item(self, key: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        response = self.table.get_item(Key=key)
        return response.get("Item")

    def update_item(
        self,
        key: Dict[str, Any],
        update_expression: str,
        expression_attribute_values: Dict[str, Any],
    ) -> None:
        self.table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )
        logger.info("Successfully updated the database")

    def delete_item(self, key: Dict[str, Any]) -> None:
        self.table.delete_item(Key=key)
        logger.info("Successfully deleted item from the database")

    def query_items(
        self, primary_key: str, key_condition_expression: str
    ) -> Optional[List[Dict[str, Any]]]:
        response = self.table.query(
            KeyConditionExpression=Key(primary_key).eq(key_condition_expression),
        )
        return response.get("Items")
