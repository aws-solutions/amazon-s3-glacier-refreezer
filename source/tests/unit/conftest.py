"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing

import boto3
import pytest

from moto import mock_s3, mock_glacier  # type: ignore


if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_glacier import GlacierClient
else:
    S3Client = object
    GlacierClient = object


@pytest.fixture(scope="module")
def aws_credentials() -> None:
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_REGION"] = "us-east-1"


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
