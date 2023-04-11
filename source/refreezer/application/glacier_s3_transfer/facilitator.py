"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING, Optional
from mypy_boto3_glacier import GlacierClient
from refreezer.application.glacier_s3_transfer.download import GlacierDownload
from refreezer.application.glacier_s3_transfer.upload import S3Upload
from refreezer.application.hashing.tree_hash import TreeHash
from refreezer.application.util.exceptions import GlacierChecksumMismatch
from base64 import b64encode


if TYPE_CHECKING:
    from mypy_boto3_glacier import GlacierClient
    from refreezer.application.model.responses import (
        GlacierRetrieval as GlacierRetrievalResponse,
    )
else:
    GlacierClient = object
    GlacierRetrievalResponse = object


class GlacierToS3Facilitator:
    def __init__(
        self,
        glacier_client: GlacierClient,
        job_id: str,
        vault_name: str,
        byte_range: str,
        glacier_object_id: str,
        s3_destination_bucket: str,
        s3_destination_key: str,
        upload_id: str,
        part_number: int,
        ignore_glacier_checksum: Optional[bool] = None,
    ) -> None:
        self.glacier_client = glacier_client
        self.job_id = job_id
        self.vault_name = vault_name
        self.byte_range = byte_range
        self.glacier_object_id = glacier_object_id

        self.s3_destination_bucket = s3_destination_bucket
        self.s3_destination_key = s3_destination_key
        self.upload_id = upload_id
        self.part_number = part_number

        self.ignore_glacier_checksum = ignore_glacier_checksum

    def transfer(self) -> GlacierRetrievalResponse:
        """
        Transfers a chunk of data from an AWS Glacier vault to an S3 bucket.

        Returns a dictionary containing information about the uploaded part.

        :return: A dictionary containing information about the uploaded part.
        :rtype: GlacierRetrievalResponse

        :raises Exception: If the checksum of the downloaded chunk of data from Glacier does not match the expected checksum.

        :raises botocore.exceptions.ClientError: If there is an error communicating with AWS.
        """
        download = GlacierDownload(
            self.glacier_client,
            self.job_id,
            self.vault_name,
            self.byte_range,
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
        part = upload.upload_part(chunk, self.part_number)
        if self.ignore_glacier_checksum is None:
            part["TreeChecksum"] = b64encode(glacier_hash.digest()).decode("ascii")
        return part
