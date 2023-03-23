"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import boto3
import typing
from base64 import b64encode, b64decode
from refreezer.application.hashing.s3_hash import S3Hash

if typing.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import (
        UploadPartOutputTypeDef,
        CompletedPartTypeDef,
        CompleteMultipartUploadOutputTypeDef,
    )
else:
    S3Client = object
    UploadPartOutputTypeDef = object
    CompletedPartTypeDef = object
    CompleteMultipartUploadOutputTypeDef = object


class S3Upload:
    def __init__(
        self,
        bucket_name: str,
        key: str,
        upload_id: str,
    ) -> None:
        self.s3: S3Client = boto3.client("s3")

        self.bucket_name = bucket_name
        self.key = key
        self.parts: list[CompletedPartTypeDef] = []
        self.upload_id = upload_id

    def upload_part(self, chunk: bytes, part_number: int) -> CompletedPartTypeDef:
        checksum = b64encode(S3Hash.hash(chunk)).decode("ascii")
        response: UploadPartOutputTypeDef = self.s3.upload_part(
            Body=chunk,
            Bucket=self.bucket_name,
            Key=self.key,
            PartNumber=part_number,
            UploadId=self.upload_id,
            ChecksumAlgorithm="SHA256",
            ChecksumSHA256=checksum,
        )
        return S3Upload._build_part(part_number, response["ETag"], checksum)

    def include_part(self, part_number: int, etag: str, checksum: str) -> None:
        self.parts.append(S3Upload._build_part(part_number, etag, checksum))

    def complete_upload(self) -> CompleteMultipartUploadOutputTypeDef:
        s3_hash = S3Hash()
        for part in self.parts:
            s3_hash.include(
                b64decode(part["ChecksumSHA256"].encode("ascii")),
                part["PartNumber"] - 1,
            )

        return self.s3.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": self.parts},
            ChecksumSHA256=b64encode(s3_hash.digest()).decode("ascii"),
        )

    @staticmethod
    def _build_part(part_number: int, etag: str, checksum: str) -> CompletedPartTypeDef:
        return {
            "PartNumber": part_number,
            "ETag": etag,
            "ChecksumSHA256": checksum,
        }
