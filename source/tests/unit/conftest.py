"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing

import boto3
import pytest

import aws_cdk as core
import aws_cdk.assertions as assertions
import cdk_nag

from moto import mock_s3, mock_glacier, mock_dynamodb  # type: ignore

from refreezer.infrastructure.stack import (
    RefreezerStack,
)


if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_glacier import GlacierClient
    from mypy_boto3_dynamodb import DynamoDBServiceResource
    from mypy_boto3_dynamodb.service_resource import Table
else:
    S3Client = object
    GlacierClient = object
    DynamoDBServiceResource = object
    DynamoDBClient = object
    Table = object


@pytest.fixture(scope="module")
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["DDB_TABLE_NAME"] = "FacilitatorTable"


@pytest.fixture(scope="module")
def s3_client(aws_credentials: None) -> typing.Iterator[S3Client]:
    with mock_s3():
        connection: S3Client = boto3.client("s3", region_name="us-east-1")
        yield connection


@pytest.fixture(scope="module")
def glacier_client(aws_credentials: None) -> typing.Iterator[GlacierClient]:
    with mock_glacier():
        connection: GlacierClient = boto3.client("glacier", region_name="us-east-1")
        yield connection


@pytest.fixture(scope="module")
def dynamodb_resource(
    aws_credentials: None,
) -> typing.Generator[DynamoDBServiceResource, None, None]:
    with mock_dynamodb():
        ddb = boto3.resource("dynamodb", "us-east-1")
        yield ddb


@pytest.fixture(scope="module")
def common_dynamodb_table_mock(
    aws_credentials: None, dynamodb_resource: DynamoDBServiceResource
) -> Table:
    dynamodb_resource.create_table(
        AttributeDefinitions=[
            {"AttributeName": "job_id", "AttributeType": "S"},
            {"AttributeName": "task_token", "AttributeType": "S"},
            {"AttributeName": "start_timestamp", "AttributeType": "S"},
        ],
        TableName=os.environ["DDB_TABLE_NAME"],
        KeySchema=[
            {"AttributeName": "job_id", "KeyType": "HASH"},
            {"AttributeName": "task_token", "KeyType": "RANGE"},
            {"AttributeName": "start_timestamp", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return dynamodb_resource.Table(os.environ["DDB_TABLE_NAME"])


@pytest.fixture
def stack() -> RefreezerStack:
    app = core.App()
    stack = RefreezerStack(app, "refreezer")
    core.Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks(log_ignores=True, verbose=True)
    )
    return stack


@pytest.fixture
def template(stack: RefreezerStack) -> assertions.Template:
    return assertions.Template.from_stack(stack)
