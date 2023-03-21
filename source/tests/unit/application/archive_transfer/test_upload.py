"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import typing
import pytest
from refreezer.application.archive_transfer.upload import S3Upload

if typing.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


def test_initiate_multipart_upload(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")

    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    assert s3_upload.upload_id is not None


def test_upload_part(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")
    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.upload_part(chunk=b"test-chunk")
    assert s3_upload.part_number == 2


def test_include_part(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")
    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.include_part(part_number=1, etag="test-etag", checksum="test-checksum")
    assert s3_upload.parts == [
        {"PartNumber": 1, "ETag": "test-etag", "ChecksumSHA256": "test-checksum"}
    ]


def test_include_part_out_of_order(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")
    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.include_part(part_number=2, etag="test-etag", checksum="test-checksum")
    s3_upload.include_part(part_number=1, etag="test-etag", checksum="test-checksum")
    assert s3_upload.parts == [
        {"PartNumber": 1, "ETag": "test-etag", "ChecksumSHA256": "test-checksum"},
        {"PartNumber": 2, "ETag": "test-etag", "ChecksumSHA256": "test-checksum"},
    ]


def test_include_part_after_complete(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")
    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.upload_part(chunk=b"test-chunk")
    s3_upload.complete_upload()
    with pytest.raises(Exception):
        s3_upload.include_part(
            part_number=2, etag="test-etag", checksum="test-checksum"
        )


def test_resume_multipart_upload_completion(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")
    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    part = s3_upload.upload_part(chunk=b"test-chunk")
    s3_upload = S3Upload(
        bucket_name="test-bucket",
        key="test-key",
        archive_id="test-archive-id",
        part_number=2,
        upload_id=s3_upload.upload_id,
    )
    s3_upload.include_part(part["PartNumber"], part["ETag"], part["ChecksumSHA256"])
    s3_upload.complete_upload()
    assert s3_upload.completed


def test_resume_multipart_upload_without_upload_id(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")
    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    part = s3_upload.upload_part(chunk=b"test-chunk")
    with pytest.raises(Exception):
        s3_upload = S3Upload(
            bucket_name="test-bucket",
            key="test-key",
            archive_id="test-archive-id",
            part_number=2,
        )


def test_complete_upload(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")

    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.upload_part(chunk=b"test-chunk")
    s3_upload.complete_upload()
    assert s3_upload.completed


def test_upload_part_after_complete(s3_client: S3Client) -> None:
    s3_client.create_bucket(Bucket="test-bucket")

    s3_upload = S3Upload(
        bucket_name="test-bucket", key="test-key", archive_id="test-archive-id"
    )
    s3_upload.upload_part(chunk=b"test-chunk")
    s3_upload.complete_upload()
    assert s3_upload.completed
    with pytest.raises(Exception):
        s3_upload.upload_part(chunk=b"test-chunk")
