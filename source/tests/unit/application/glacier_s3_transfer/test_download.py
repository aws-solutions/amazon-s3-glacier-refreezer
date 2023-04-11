"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
import pytest
from typing import TYPE_CHECKING, Tuple
from unittest.mock import patch
from datetime import timedelta
from refreezer.application.glacier_s3_transfer.download import GlacierDownload
from refreezer.application.util.exceptions import AccessViolation

if TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import InitiateJobOutputTypeDef
else:
    GlacierClient = object
    InitiateJobOutputTypeDef = object

TEST_DATA = b"test"


def test_init(setup_glacier_job: Tuple[GlacierClient, str]) -> None:
    vault_name: str = "vault_name"
    byte_range: str = "0-1024"

    # Create a GlacierDownload object
    client, job_id = setup_glacier_job
    download = GlacierDownload(client, job_id, vault_name, byte_range)

    # Test that the object was initialized correctly
    assert download.params["jobId"] == job_id
    assert download.params["vaultName"] == vault_name
    assert download.params["range"] == f"bytes={byte_range}"


def test_read_correctness(setup_glacier_job: Tuple[GlacierClient, str]) -> None:
    client, job_id = setup_glacier_job
    download = GlacierDownload(client, job_id, "vault_name", "0-1024")

    # Test that read() method returns the correct chunks
    chunk: bytes = download.read()
    assert chunk == TEST_DATA


def test_read_prevents_second_access(
    setup_glacier_job: Tuple[GlacierClient, str]
) -> None:
    client, job_id = setup_glacier_job
    download = GlacierDownload(client, job_id, "vault_name", "0-1024")
    download.read()
    with pytest.raises(AccessViolation):
        download.read()


@pytest.fixture
def setup_glacier_job(
    glacier_client: GlacierClient,
) -> Tuple[GlacierClient, str]:
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
    return (glacier_client, job_response["jobId"])
