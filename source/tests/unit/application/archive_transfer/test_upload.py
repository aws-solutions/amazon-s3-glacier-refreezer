"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import boto3
import typing
from refreezer.application.archive_transfer.upload import S3Upload
from moto import mock_s3  # type: ignore

if typing.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


@mock_s3  # type: ignore[misc]
def test_initiate_multipart_upload() -> None:
    s3: S3Client = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")

    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    assert s3_upload.upload_id is not None


@mock_s3  # type: ignore[misc]
def test_upload_part() -> None:
    s3: S3Client = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")

    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.upload_part(chunk=b"test-chunk")
    assert s3_upload.part_number == 2


@mock_s3  # type: ignore[misc]
def test_complete_upload() -> None:
    s3: S3Client = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")

    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.upload_part(chunk=b"test-chunk")
    s3_upload.complete_upload()
    assert s3_upload.completed is True


@mock_s3  # type: ignore[misc]
def test_upload_part_after_complete() -> None:
    s3: S3Client = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")

    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.upload_part(chunk=b"test-chunk")
    s3_upload.complete_upload()
    assert s3_upload.completed is True
    try:
        s3_upload.upload_part(chunk=b"test-chunk")
    except Exception as e:
        assert e.args[0] == "Upload already completed"
