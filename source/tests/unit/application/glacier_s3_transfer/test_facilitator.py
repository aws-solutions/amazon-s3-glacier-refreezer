"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import unittest
from unittest.mock import patch, MagicMock
from refreezer.application.glacier_s3_transfer.facilitator import (
    GlacierToS3Facilitator,
)
from refreezer.application.util.exceptions import GlacierChecksumMismatch
from typing import Callable, Any, Optional


class TestGlacierToS3Facilitator(unittest.TestCase):
    def setUp(self) -> None:
        self.job_id = "job1"
        self.vault_name = "vault1"
        self.start_byte = 0
        self.end_byte = 100
        self.glacier_object_id = "archive1"
        self.s3_destination_bucket = "bucket1"
        self.s3_destination_key = "key1"
        self.upload_id = "upload1"
        self.part_number = 1
        self.mock_upload: Callable[
            [bytes, int], dict[str, Any]
        ] = lambda data, part_number: {
            "ETag": "etag1",
            "PartNumber": part_number,
            "Checksum": data,
        }

    def test_init(self) -> None:
        facilitator = self.create_facilitator()
        self.assertEqual(facilitator.job_id, self.job_id)
        self.assertEqual(facilitator.vault_name, self.vault_name)
        self.assertEqual(facilitator.start_byte, self.start_byte)
        self.assertEqual(facilitator.end_byte, self.end_byte)
        self.assertEqual(facilitator.glacier_object_id, self.glacier_object_id)
        self.assertEqual(facilitator.s3_destination_bucket, self.s3_destination_bucket)
        self.assertEqual(facilitator.s3_destination_key, self.s3_destination_key)
        self.assertEqual(facilitator.upload_id, self.upload_id)
        self.assertEqual(facilitator.part_number, self.part_number)

    @patch("refreezer.application.glacier_s3_transfer.facilitator.TreeHash")
    @patch("refreezer.application.glacier_s3_transfer.facilitator.GlacierDownload")
    @patch("refreezer.application.glacier_s3_transfer.facilitator.S3Upload")
    def test_transfer_happy_path(
        self,
        upload_mock: MagicMock,
        download_mock: MagicMock,
        tree_hash_mock: MagicMock,
    ) -> None:
        download_instance = download_mock.return_value
        data = b"chunk"
        download_instance.read.return_value = data
        download_instance.checksum.return_value = (
            "deadbeef"  # An example checksum which only has hex characters
        )

        tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

        facilitator = self.create_facilitator()
        upload_mock.return_value.upload_part = self.mock_upload
        result = facilitator.transfer()
        expected_result = self.mock_upload(data, self.part_number)
        self.assertCountEqual(result, expected_result)

    @patch("refreezer.application.glacier_s3_transfer.facilitator.TreeHash")
    @patch("refreezer.application.glacier_s3_transfer.facilitator.GlacierDownload")
    @patch("refreezer.application.glacier_s3_transfer.facilitator.S3Upload")
    def test_transfer_glacier_checksum_mismatch(
        self,
        upload_mock: MagicMock,
        download_mock: MagicMock,
        tree_hash_mock: MagicMock,
    ) -> None:
        download_instance = download_mock.return_value
        download_instance.read.return_value = b"chunk"
        download_instance.checksum.return_value = (
            "deadbeef1"  # An example checksum which only has hex characters
        )

        tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

        facilitator = self.create_facilitator()
        upload_mock.return_value.upload_part = self.mock_upload
        with self.assertRaises(GlacierChecksumMismatch):
            facilitator.transfer()

    @patch("refreezer.application.glacier_s3_transfer.facilitator.TreeHash")
    @patch("refreezer.application.glacier_s3_transfer.facilitator.GlacierDownload")
    @patch("refreezer.application.glacier_s3_transfer.facilitator.S3Upload")
    def test_transfer_glacier_checksum_mismatch_disabled(
        self,
        upload_mock: MagicMock,
        download_mock: MagicMock,
        tree_hash_mock: MagicMock,
    ) -> None:
        download_instance = download_mock.return_value
        download_instance.read.return_value = b"chunk"
        download_instance.checksum.return_value = (
            "deadbeef1"  # An example checksum which only has hex characters
        )

        tree_hash_mock.return_value.digest.return_value = b"\xde\xad\xbe\xef"

        facilitator = self.create_facilitator(True)
        upload_mock.return_value.upload_part = self.mock_upload
        facilitator.transfer()

    def create_facilitator(
        self, ignore_glacier_checksum: Optional[bool] = None
    ) -> GlacierToS3Facilitator:
        return GlacierToS3Facilitator(
            self.job_id,
            self.vault_name,
            self.start_byte,
            self.end_byte,
            self.glacier_object_id,
            self.s3_destination_bucket,
            self.s3_destination_key,
            self.upload_id,
            self.part_number,
            ignore_glacier_checksum,
        )
