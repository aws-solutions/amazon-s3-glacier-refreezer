"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


import os
import moto  # type: ignore
import json
import pytest
import logging

from unittest.mock import Mock
from typing import TYPE_CHECKING, Dict, Any, Optional
from refreezer.application.facilitator.processor import sns_handler, dynamoDb_handler

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table
else:
    Table = object


logger = logging.getLogger()


@pytest.fixture
def job_id() -> str:
    return "KXt2zItqLEKWXWyHk__7sVM8PfNIrdrsdtTLMPsyzXMnIriEK4lzltZgN7erM6_-VLXwOioQapa8EOgKfqTpqeGWuGpk"


def sns_record(
    glacier_job_result: Dict[str, Any], status_code: Optional[str] = None
) -> Dict[str, Any]:
    if status_code:
        glacier_job_result["StatusCode"] = status_code
    return {
        "EventSource": "aws:sns",
        "Sns": {
            "Message": json.dumps(glacier_job_result),
            "Timestamp": "12:12:12",
        },
    }


@pytest.fixture
def sns_default_record(glacier_job_result: Dict[str, Any]) -> Dict[str, Any]:
    return sns_record(glacier_job_result)


@pytest.fixture
def sns_failed_record(glacier_job_result: Dict[str, Any]) -> Dict[str, Any]:
    return sns_record(glacier_job_result, "Failed")


@pytest.fixture
def sns_in_progress_record(glacier_job_result: Dict[str, Any]) -> Dict[str, Any]:
    return sns_record(glacier_job_result, "InProgress")


@pytest.fixture
def table_with_item(common_dynamodb_table_mock: Table, job_id: str) -> None:
    common_dynamodb_table_mock.put_item(
        Item={
            "job_id": job_id,
            "task_token": "task_token",
            "start_timestamp": "11:11:11",
        }
    )


@pytest.fixture
def table_with_item_missing_token(
    common_dynamodb_table_mock: Table, job_id: str
) -> None:
    common_dynamodb_table_mock.put_item(
        Item={
            "job_id": job_id,
            "start_timestamp": "11:11:11",
        }
    )


@pytest.fixture
def dynamodb_default_record() -> Dict[str, Any]:
    return {
        "EventSource": "aws:dynamodb",
        "dynamodb": {
            "NewImage": {
                "job_id": {"S": "ids"},
                "finish_timestamp": {"S": "12:12:12"},
                "task_token": {"S": "task_token"},
                "job_result": {
                    "M": {
                        "AnotherStr": {"S": "Str"},
                        "StatusCode": {"S": "Succeeded"},
                        "Size": {"N": "1024"},
                    }
                },
            },
        },
    }


@pytest.fixture
def dynamodb_failure_record(dynamodb_default_record: Dict[str, Any]) -> Dict[str, Any]:
    dynamodb_default_record["dynamodb"]["NewImage"]["job_result"]["M"]["StatusCode"][
        "S"
    ] = "Failed"
    return dynamodb_default_record


def test_sns_handler_success(
    caplog: pytest.LogCaptureFixture,
    table_with_item: Table,
    sns_default_record: Dict[str, Any],
) -> None:
    with caplog.at_level(logging.INFO):
        sns_handler(sns_default_record, Mock())
        assert "Successfully inserted item into the database" in caplog.text


def test_sns_failure(
    table_with_item: Table,
    caplog: pytest.LogCaptureFixture,
    sns_failed_record: Dict[str, Any],
) -> None:
    with caplog.at_level(logging.INFO):
        sns_handler(sns_failed_record, Mock())
        assert "has not succeeded" in caplog.text


def test_missing_token(
    sns_default_record: Dict[str, Any],
    table_with_item_missing_token: Table,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO):
        sns_handler(sns_default_record, Mock())
        assert "Cannot find the task token" in caplog.text


def test_sns_with_in_progress_job(
    table_with_item: Table,
    sns_in_progress_record: Dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO):
        sns_handler(sns_in_progress_record, Mock())
        assert "is still in progress" in caplog.text


def test_dynamo_handler_success(
    caplog: pytest.LogCaptureFixture, dynamodb_default_record: Dict[str, Any]
) -> None:
    with caplog.at_level(logging.INFO):
        dynamoDb_handler(dynamodb_default_record, Mock())
        assert "Succesfully updated the database" in caplog.text


def test_dynamo_handler_failure(
    dynamodb_failure_record: Dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.INFO):
        dynamoDb_handler(dynamodb_failure_record, Mock())
        assert "FAILED::" in caplog.text
