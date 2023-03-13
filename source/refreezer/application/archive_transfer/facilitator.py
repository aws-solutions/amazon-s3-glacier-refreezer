"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Optional
from refreezer.application.archive_transfer.download import GlacierDownload
from refreezer.application.archive_transfer.upload import S3Upload
from concurrent import futures


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

    def transfer_archive(self) -> None:
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

        with futures.ThreadPoolExecutor(max_workers=2) as upload_executor:
            upload_futures = [
                upload_executor.submit(archive_upload.upload_part, chunk)
                for chunk in archive_download
            ]
            for future in upload_futures:
                future.result()
