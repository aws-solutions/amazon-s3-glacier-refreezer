"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING, Optional
from refreezer.application.archive_transfer.download import GlacierDownload
from refreezer.application.archive_transfer.upload import S3Upload
from refreezer.application.hashing.tree_hash import TreeHash
from concurrent import futures


if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import (
        CompletedPartTypeDef,
    )
else:
    CompletedPartTypeDef = object

MAX_UPLOAD_WORKERS = 2


class ArchiveTransferFacilitator:
    def __init__(
        self,
        job_id: str,
        vault_name: str,
        start_byte: int,
        end_byte: int,
        chunk_size: int,
        destination_bucket: str,
        archive_key: str,
        archive_id: str,
        upload_id: Optional[str],
        part_number: Optional[int],
    ) -> None:
        self.job_id = job_id
        self.vault_name = vault_name
        self.start_byte = start_byte
        self.end_byte = end_byte
        self.chunk_size = chunk_size

        self.destination_bucket = destination_bucket
        self.archive_key = archive_key
        self.archive_id = archive_id
        self.upload_id = upload_id
        self.part_number = part_number

    def transfer_archive(self) -> list[CompletedPartTypeDef]:
        archive_download = GlacierDownload(
            self.job_id,
            self.vault_name,
            self.start_byte,
            self.end_byte,
            self.chunk_size,
        )
        archive_upload = S3Upload(
            self.destination_bucket,
            self.archive_key,
            self.archive_id,
            self.upload_id,
            self.part_number,
        )
        with futures.ThreadPoolExecutor(
            max_workers=MAX_UPLOAD_WORKERS
        ) as upload_executor:
            glacier_hash = TreeHash()
            upload_futures = set()
            completed_futures = set()
            for chunk in archive_download:
                upload_futures.add(
                    upload_executor.submit(archive_upload.upload_part, chunk)
                )
                glacier_hash.update(chunk)
                if len(upload_futures) > MAX_UPLOAD_WORKERS:
                    completed, upload_futures = futures.wait(
                        upload_futures, return_when=futures.FIRST_COMPLETED
                    )
                    completed_futures.update(completed)
            completed, _ = futures.wait(
                upload_futures, return_when=futures.ALL_COMPLETED
            )
            completed_futures.update(completed)
        if glacier_hash.digest().hex() != archive_download.checksum():
            raise Exception("Glacier checksum mismatch")
        return [future.result() for future in completed_futures]
