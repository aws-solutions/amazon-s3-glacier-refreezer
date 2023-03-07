"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import boto3
from unittest.mock import patch
from moto import mock_glacier  # type: ignore
from datetime import timedelta
from refreezer.application.archive_transfer.download import GlacierDownload
from typing import Any


@mock_glacier  # type: ignore[misc]
def test_init() -> None:
    job_id = setup_glacier()
    vault_name = "vault_name"
    start_byte = 0
    end_byte = 1024
    chunk_size = 256

    # Create a GlacierDownload object
    download = GlacierDownload(job_id, vault_name, start_byte, end_byte, chunk_size)

    # Test that the object was initialized correctly
    assert download.params["jobId"] == job_id
    assert download.params["vaultName"] == vault_name
    assert download.params["range"] == f"bytes={start_byte}-{end_byte}"
    assert download.chunk_size == chunk_size


@mock_glacier  # type: ignore[misc]
def test_iter() -> None:
    test_response = b"test"
    job_id = setup_glacier(test_response)

    # Test the iter() method of GlacierDownload object
    download = GlacierDownload(job_id, "vault_name", 0, 1024, 1)

    # Test that iter() method returns the correct chunks
    chunks = []
    for chunk in download.iter():
        chunks.append(chunk)
    assert len(chunks) == len(test_response)
    assert b"".join(chunks) == test_response


def setup_glacier(test_body: bytes = b"test") -> Any:
    glacier = boto3.client("glacier", region_name="us-east-1")
    glacier.create_vault(vaultName="vault_name")
    archive_response = glacier.upload_archive(vaultName="vault_name", body=test_body)
    print(archive_response)
    archive_id = archive_response["archiveId"]
    with patch("datetime.timedelta", return_value=timedelta(seconds=0)):
        job_response = glacier.initiate_job(
            vaultName="vault_name",
            jobParameters={
                "Type": "archive-retrieval",
                "ArchiveId": archive_id,
                "Tier": "Expedited",
            },
        )
    return job_response["jobId"]
