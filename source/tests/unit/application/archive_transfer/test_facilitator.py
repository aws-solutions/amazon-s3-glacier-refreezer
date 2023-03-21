"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import unittest
from unittest.mock import patch, MagicMock
from refreezer.application.archive_transfer.facilitator import (
    ArchiveTransferFacilitator,
)
from typing import Callable, Any


class TestArchiveTransferFacilitator(unittest.TestCase):
    def setUp(self) -> None:
        self.job_id = "job1"
        self.vault_name = "vault1"
        self.start_byte = 0
        self.end_byte = 100
        self.chunk_size = 10
        self.destination_bucket = "bucket1"
        self.archive_key = "key1"
        self.archive_id = "archive1"
        self.upload_id = "upload1"
        self.part_number = 1
        self.mock_upload: Callable[[bytes], dict[str, Any]] = lambda data: {
            "ETag": "etag1",
            "PartNumber": 1,
            "Checksum": data,
        }

    def test_init(self) -> None:
        facilitator = self.create_facilitator()
        self.assertEqual(facilitator.job_id, self.job_id)
        self.assertEqual(facilitator.vault_name, self.vault_name)
        self.assertEqual(facilitator.start_byte, self.start_byte)
        self.assertEqual(facilitator.end_byte, self.end_byte)
        self.assertEqual(facilitator.chunk_size, self.chunk_size)
        self.assertEqual(facilitator.destination_bucket, self.destination_bucket)
        self.assertEqual(facilitator.archive_key, self.archive_key)
        self.assertEqual(facilitator.archive_id, self.archive_id)
        self.assertEqual(facilitator.upload_id, self.upload_id)
        self.assertEqual(facilitator.part_number, self.part_number)

    @patch("refreezer.application.archive_transfer.facilitator.TreeHash")
    @patch("refreezer.application.archive_transfer.facilitator.GlacierDownload")
    @patch("refreezer.application.archive_transfer.facilitator.S3Upload")
    def test_transfer_archive_happy_path(
        self,
        upload_mock: MagicMock,
        download_mock: MagicMock,
        tree_hash_mock: MagicMock,
    ) -> None:
        download_instance = download_mock.return_value
        data = [b"chunk1", b"chunk2"]
        download_instance.__iter__.return_value = iter(data)
        download_instance.checksum.return_value = (
            "deadbeef"  # An example checksum which only has hex characters
        )

        tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

        facilitator = self.create_facilitator()
        upload_mock.return_value.upload_part = self.mock_upload
        results = facilitator.transfer_archive()
        expected_results = [self.mock_upload(chunk) for chunk in data]
        self.assertCountEqual(results, expected_results)

    @patch("refreezer.application.archive_transfer.facilitator.TreeHash")
    @patch("refreezer.application.archive_transfer.facilitator.GlacierDownload")
    @patch("refreezer.application.archive_transfer.facilitator.S3Upload")
    def test_transfer_archive_blocked_download(
        self,
        upload_mock: MagicMock,
        download_mock: MagicMock,
        tree_hash_mock: MagicMock,
    ) -> None:
        download_instance = download_mock.return_value
        # The download will wait since there are more than 2 chunks
        data = [b"chunk1", b"chunk2", b"chunk3", b"chunk4"]
        download_instance.__iter__.return_value = iter(data)
        download_instance.checksum.return_value = (
            "deadbeef"  # An example checksum which only has hex characters
        )

        tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

        facilitator = self.create_facilitator()
        upload_mock.return_value.upload_part = self.mock_upload
        results = facilitator.transfer_archive()
        expected_results = [self.mock_upload(chunk) for chunk in data]
        self.assertCountEqual(results, expected_results)

    @patch("refreezer.application.archive_transfer.facilitator.TreeHash")
    @patch("refreezer.application.archive_transfer.facilitator.GlacierDownload")
    @patch("refreezer.application.archive_transfer.facilitator.S3Upload")
    def test_transfer_archive_glacier_checksum_mismatch(
        self,
        upload_mock: MagicMock,
        download_mock: MagicMock,
        tree_hash_mock: MagicMock,
    ) -> None:
        download_instance = download_mock.return_value
        download_instance.__iter__.return_value = iter([b"chunk1", b"chunk2"])
        download_instance.checksum.return_value = (
            "1deadbeef"  # An example checksum which only has hex characters
        )

        tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

        facilitator = self.create_facilitator()
        upload_mock.return_value.upload_part = self.mock_upload
        with self.assertRaises(Exception):
            facilitator.transfer_archive()

    def create_facilitator(self) -> ArchiveTransferFacilitator:
        return ArchiveTransferFacilitator(
            self.job_id,
            self.vault_name,
            self.start_byte,
            self.end_byte,
            self.chunk_size,
            self.destination_bucket,
            self.archive_key,
            self.archive_id,
            self.upload_id,
            self.part_number,
        )
