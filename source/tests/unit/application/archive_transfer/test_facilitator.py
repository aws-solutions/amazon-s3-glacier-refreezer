"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import unittest
from unittest.mock import patch, MagicMock
from refreezer.application.archive_transfer.facilitator import (
    ArchiveTransferFacilitator,
)


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

    def test_init(self) -> None:
        facilitator = ArchiveTransferFacilitator(
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

    @patch("refreezer.application.archive_transfer.facilitator.GlacierDownload")
    @patch("refreezer.application.archive_transfer.facilitator.S3Upload")
    def test_transfer_archive(
        self, upload_mock: MagicMock, download_mock: MagicMock
    ) -> None:
        download_mock.return_value = iter([b"chunk1", b"chunk2"])
        facilitator = ArchiveTransferFacilitator(
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
        with patch(
            "refreezer.application.archive_transfer.facilitator.futures.ThreadPoolExecutor"
        ) as thread_pool_mock:
            future_mock = MagicMock()
            future_mock.result.return_value = None
            submit_mock = thread_pool_mock.return_value.__enter__.return_value.submit
            submit_mock.return_value = future_mock

            facilitator.transfer_archive()
            submit_mock.assert_any_call(upload_mock.return_value.upload_part, b"chunk1")
            submit_mock.assert_any_call(upload_mock.return_value.upload_part, b"chunk2")
            self.assertEqual(submit_mock.call_count, 2)
