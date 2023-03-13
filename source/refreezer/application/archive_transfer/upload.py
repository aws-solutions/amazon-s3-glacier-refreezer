"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import boto3
import typing

if typing.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import (
        UploadPartOutputTypeDef,
        CreateMultipartUploadOutputTypeDef,
        CompletedPartTypeDef,
    )
else:
    S3Client = object
    CreateMultipartUploadOutputTypeDef = object
    UploadPartOutputTypeDef = object
    CompletedPartTypeDef = object


class S3Upload:
    def __init__(
        self,
        bucket_name: str,
        key: str,
        archive_id: str,
        upload_id: typing.Optional[str] = None,
        part_number: typing.Optional[int] = None,
    ) -> None:
        self.s3: S3Client = boto3.client("s3")

        self.bucket_name = bucket_name
        self.key = key
        self.archive_id = archive_id
        self.parts: list[CompletedPartTypeDef] = []

        self.part_number = part_number or 1
        self.upload_id = upload_id or self._initiate_multipart_upload()

        self.completed: bool = False

    def _initiate_multipart_upload(self) -> str:
        response: CreateMultipartUploadOutputTypeDef = self.s3.create_multipart_upload(
            Bucket=self.bucket_name, Key=self.key
        )
        return response["UploadId"]

    def upload_part(self, chunk: bytes) -> None:
        if self.completed:
            raise Exception("Upload already completed")
        part_number = self.part_number
        self.part_number += 1
        response: UploadPartOutputTypeDef = self.s3.upload_part(
            Body=chunk,
            Bucket=self.bucket_name,
            Key=self.key,
            PartNumber=part_number,
            UploadId=self.upload_id,
        )
        self.parts.insert(
            part_number - 1, {"PartNumber": part_number, "ETag": response["ETag"]}
        )

    def complete_upload(self) -> None:
        self.s3.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": self.parts},
        )
        self.completed = True
