"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import boto3

from refreezer.infrastructure.stack import OutputKeys

if typing.TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:
    S3Client = object


def assert_bucket_put_get_object(bucket_name: str) -> None:
    client: S3Client = boto3.client("s3")

    value = "test data".encode("utf-8")
    key = "test_object.txt"
    client.put_object(Bucket=bucket_name, Key=key, Body=value)

    assert value == client.get_object(Bucket=bucket_name, Key=key)["Body"].read()

    client.delete_object(Bucket=bucket_name, Key=key)


def test_output_bucket_put_get_object() -> None:
    bucket_name = os.environ[OutputKeys.OUTPUT_BUCKET_NAME]
    assert_bucket_put_get_object(bucket_name)


def test_inventory_bucket_put_get_object() -> None:
    bucket_name = os.environ[OutputKeys.INVENTORY_BUCKET_NAME]
    assert_bucket_put_get_object(bucket_name)
