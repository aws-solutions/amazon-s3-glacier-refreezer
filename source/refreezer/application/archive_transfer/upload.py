"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import boto3
from typing import Optional, Any


class S3Upload:
    def __init__(
        self,
        bucket_name: str,
        key: str,
        archive_id: str,
        upload_id: Optional[str] = None,
        part_number: int = 1,
    ) -> None:
        self.s3 = boto3.client("s3")
        self.bucket_name = bucket_name
        self.key = key
        self.archive_id = archive_id
        self.part_number = part_number
        self.part_info: list[dict[str, int]] = []
        self.upload_id = upload_id or self._initiate_multipart_upload()
        self.completed = False

    def _initiate_multipart_upload(self) -> Any:
        response = self.s3.create_multipart_upload(
            Bucket=self.bucket_name, Key=self.key, ChecksumAlgorithm="SHA256"
        )
        return response["UploadId"]

    def upload_part(self, chunk: bytes) -> None:
        if self.completed:
            raise Exception("Upload already completed")
        response = self.s3.upload_part(
            Body=chunk,
            Bucket=self.bucket_name,
            Key=self.key,
            PartNumber=self.part_number,
            UploadId=self.upload_id,
        )
        self.part_info.append(
            {"PartNumber": self.part_number, "ETag": response["ETag"]}
        )
        # self._update_part_info(self.archive_id, self.part_number, response['ETag'], response['ChecksumSHA256'])
        self.part_number += 1

    def complete_upload(self) -> None:
        self.s3.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": self.part_info},
        )
        self.completed = True
