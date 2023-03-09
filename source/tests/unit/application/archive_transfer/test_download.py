"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import boto3
import pytest
import typing
from unittest.mock import patch
from datetime import timedelta
from refreezer.application.archive_transfer.download import GlacierDownload
from moto import mock_glacier  # type: ignore

if typing.TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import InitiateJobOutputTypeDef
else:
    GlacierClient = object
    InitiateJobOutputTypeDef = object


@mock_glacier  # type: ignore[misc]
def test_init() -> None:
    job_id = setup_glacier()
    vault_name: str = "vault_name"
    start_byte: int = 0
    end_byte: int = 1024
    chunk_size: int = 256

    # Create a GlacierDownload object
    download = GlacierDownload(job_id, vault_name, start_byte, end_byte, chunk_size)

    # Test that the object was initialized correctly
    assert download.params["jobId"] == job_id
    assert download.params["vaultName"] == vault_name
    assert download.params["range"] == f"bytes={start_byte}-{end_byte}"
    assert download.chunk_size == chunk_size


@mock_glacier  # type: ignore[misc]
def test_iter_correctness() -> None:
    test_response: bytes = b"test"
    job_id = setup_glacier(test_response)
    download = GlacierDownload(job_id, "vault_name", 0, 1024, 1)

    # Test that iter() method returns the correct chunks
    chunks: list[bytes] = []
    for chunk in download:
        chunks.append(chunk)
    assert len(chunks) == len(test_response)
    assert b"".join(chunks) == test_response


@mock_glacier  # type: ignore[misc]
def test_iter_prevents_second_access() -> None:
    job_id = setup_glacier()
    download = GlacierDownload(job_id, "vault_name", 0, 1024, 1)

    for _ in download:
        pass
    with pytest.raises(Exception):
        for _ in download:
            pass


def setup_glacier(test_body: bytes = b"test") -> str:
    glacier: GlacierClient = boto3.client("glacier", region_name="us-east-1")
    glacier.create_vault(vaultName="vault_name")
    archive_response = glacier.upload_archive(vaultName="vault_name", body=test_body)
    archive_id = archive_response["archiveId"]
    with patch("datetime.timedelta", return_value=timedelta(seconds=0)):
        job_response: InitiateJobOutputTypeDef = glacier.initiate_job(
            vaultName="vault_name",
            jobParameters={
                "Type": "archive-retrieval",
                "ArchiveId": archive_id,
                "Tier": "Expedited",
            },
        )
    return job_response["jobId"]
