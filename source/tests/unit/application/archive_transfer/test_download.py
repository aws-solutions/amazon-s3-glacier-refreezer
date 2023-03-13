"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import pytest
import typing
from unittest.mock import patch
from datetime import timedelta
from refreezer.application.archive_transfer.download import GlacierDownload

if typing.TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import InitiateJobOutputTypeDef
else:
    GlacierClient = object
    InitiateJobOutputTypeDef = object

TEST_DATA = b"test"


def test_init(setup_glacier_job: str) -> None:
    vault_name: str = "vault_name"
    start_byte: int = 0
    end_byte: int = 1024
    chunk_size: int = 256

    # Create a GlacierDownload object
    download = GlacierDownload(
        setup_glacier_job, vault_name, start_byte, end_byte, chunk_size
    )

    # Test that the object was initialized correctly
    assert download.params["jobId"] == setup_glacier_job
    assert download.params["vaultName"] == vault_name
    assert download.params["range"] == f"bytes={start_byte}-{end_byte}"
    assert download.chunk_size == chunk_size


def test_iter_correctness(setup_glacier_job: str) -> None:
    download = GlacierDownload(setup_glacier_job, "vault_name", 0, 1024, 1)

    # Test that iter() method returns the correct chunks
    chunks: list[bytes] = []
    for chunk in download:
        chunks.append(chunk)
    assert len(chunks) == len(TEST_DATA)
    assert b"".join(chunks) == TEST_DATA


def test_iter_prevents_second_access(setup_glacier_job: str) -> None:
    download = GlacierDownload(setup_glacier_job, "vault_name", 0, 1024, 1)

    for _ in download:
        pass
    with pytest.raises(Exception):
        for _ in download:
            pass


@pytest.fixture
def setup_glacier_job(glacier_client: GlacierClient) -> str:
    glacier_client.create_vault(vaultName="vault_name")
    archive_response = glacier_client.upload_archive(
        vaultName="vault_name", body=TEST_DATA
    )
    archive_id = archive_response["archiveId"]
    with patch("datetime.timedelta", return_value=timedelta(seconds=0)):
        job_response: InitiateJobOutputTypeDef = glacier_client.initiate_job(
            vaultName="vault_name",
            jobParameters={
                "Type": "archive-retrieval",
                "ArchiveId": archive_id,
                "Tier": "Expedited",
            },
        )
    return job_response["jobId"]
