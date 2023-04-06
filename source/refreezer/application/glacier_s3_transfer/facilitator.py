"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING, Optional
from refreezer.application.glacier_s3_transfer.download import GlacierDownload
from refreezer.application.glacier_s3_transfer.upload import S3Upload
from refreezer.application.hashing.tree_hash import TreeHash
from refreezer.application.util.exceptions import GlacierChecksumMismatch


if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import (
        CompletedPartTypeDef,
    )
else:
    CompletedPartTypeDef = object


class GlacierToS3Facilitator:
    def __init__(
        self,
        job_id: str,
        vault_name: str,
        start_byte: int,
        end_byte: int,
        glacier_object_id: str,
        s3_destination_bucket: str,
        s3_destination_key: str,
        upload_id: str,
        part_number: int,
        ignore_glacier_checksum: Optional[bool] = None,
    ) -> None:
        self.job_id = job_id
        self.vault_name = vault_name
        self.start_byte = start_byte
        self.end_byte = end_byte

        self.glacier_object_id = glacier_object_id

        self.s3_destination_bucket = s3_destination_bucket
        self.s3_destination_key = s3_destination_key
        self.upload_id = upload_id
        self.part_number = part_number

        self.ignore_glacier_checksum = ignore_glacier_checksum

    def transfer(self) -> CompletedPartTypeDef:
        download = GlacierDownload(
            self.job_id,
            self.vault_name,
            self.start_byte,
            self.end_byte,
        )
        upload = S3Upload(
            self.s3_destination_bucket,
            self.s3_destination_key,
            self.upload_id,
        )
        chunk = download.read()
        if self.ignore_glacier_checksum is None:
            glacier_hash = TreeHash()
            glacier_hash.update(chunk)
            if glacier_hash.digest().hex() != download.checksum():
                raise GlacierChecksumMismatch()
        return upload.upload_part(chunk, self.part_number)
