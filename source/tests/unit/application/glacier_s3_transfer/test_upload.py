"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import typing
import pytest
from refreezer.application.glacier_s3_transfer.upload import S3Upload

if typing.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


def test_build_part() -> None:
    assert S3Upload._build_part(
        part_number=1, etag="test-etag", checksum="test-checksum"
    ) == {"PartNumber": 1, "ETag": "test-etag", "ChecksumSHA256": "test-checksum"}


def test_upload_part(s3_upload: S3Upload) -> None:
    part = s3_upload.upload_part(b"test-chunk", 1)
    assert (
        part["ETag"] == '"fcc3a94689b683c3186fd3ba78d8b137"'
    )  # MD5 hash of "test-chunk"
    assert part["PartNumber"] == 1


def test_upload_part_2(s3_upload: S3Upload) -> None:
    part = s3_upload.upload_part(b"test-chunk", 2)
    assert (
        part["ETag"] == '"fcc3a94689b683c3186fd3ba78d8b137"'
    )  # MD5 hash of "test-chunk"
    assert part["PartNumber"] == 2


def test_include_part(s3_upload: S3Upload) -> None:
    s3_upload.include_part(part_number=1, etag="test-etag", checksum="test-checksum")
    assert s3_upload.parts == [
        {"PartNumber": 1, "ETag": "test-etag", "ChecksumSHA256": "test-checksum"}
    ]


def test_resume_multipart_upload_completion(s3_upload: S3Upload) -> None:
    part = s3_upload.upload_part(b"test-chunk", 1)
    s3_upload = S3Upload(
        bucket_name="test-bucket",
        key="test-key",
        upload_id=s3_upload.upload_id,
    )
    s3_upload.include_part(part["PartNumber"], part["ETag"], part["ChecksumSHA256"])
    completion = s3_upload.complete_upload()
    assert completion["Bucket"] == "test-bucket"
    assert completion["Key"] == "test-key"
    assert completion["ETag"][-2] == "1"  # Assert that there was only 1 part included


def test_complete_upload_single_part(s3_upload: S3Upload) -> None:
    part = s3_upload.upload_part(b"test-chunk", 1)
    s3_upload.include_part(part["PartNumber"], part["ETag"], part["ChecksumSHA256"])
    completion = s3_upload.complete_upload()
    assert completion["Bucket"] == "test-bucket"
    assert completion["Key"] == "test-key"
    assert completion["ETag"][-2] == "1"  # Assert that there was only 1 part included


def test_complete_upload_dual_parts(s3_upload: S3Upload) -> None:
    part = s3_upload.upload_part(
        b"test-chunk" * 2**20, 1
    )  # Upload larger chunk to avoid EntityTooSmall exception
    s3_upload.include_part(part["PartNumber"], part["ETag"], part["ChecksumSHA256"])
    part = s3_upload.upload_part(b"test-chunk2", 2)
    s3_upload.include_part(part["PartNumber"], part["ETag"], part["ChecksumSHA256"])
    completion = s3_upload.complete_upload()
    assert completion["Bucket"] == "test-bucket"
    assert completion["Key"] == "test-key"
    assert completion["ETag"][-2] == "2"  # Assert that there were 2 parts included


@pytest.fixture
def s3_upload(s3_client: S3Client) -> S3Upload:
    s3_client.create_bucket(Bucket="test-bucket")
    multipart = s3_client.create_multipart_upload(
        Bucket="test-bucket", Key="test-key", ChecksumAlgorithm="SHA256"
    )
    return S3Upload(
        bucket_name="test-bucket", key="test-key", upload_id=multipart["UploadId"]
    )
