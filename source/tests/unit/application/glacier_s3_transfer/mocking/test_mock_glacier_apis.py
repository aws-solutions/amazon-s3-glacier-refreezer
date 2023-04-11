"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import typing
from refreezer.application.glacier_service.glacier_apis_factory import (
    GlacierAPIsFactory,
)

if typing.TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import GetJobOutputOutputTypeDef
else:
    GlacierClient = object
    GetJobOutputOutputTypeDef = object
import pytest


@pytest.fixture
def glacier_apis() -> GlacierClient:
    return GlacierAPIsFactory.create_instance(True)


def test_vault1_inventory_retrieval(glacier_apis: GlacierClient) -> None:
    initiate_job_response = glacier_apis.initiate_job(
        vaultName="vault1",
        accountId="123456789012",
        jobParameters={"Type": "inventory-retrieval"},
    )
    assert (
        initiate_job_response["location"]
        == "//vaults/vault1/jobs/XYRPIPBXI8YIFXQDR82UXKHDT03L8JY03398U1I5EMVCGIL9AYUAD9AZN2N582OGQPGG9XD89A2N245SW3N443RNV8H8"
    )
    assert (
        initiate_job_response["jobId"]
        == "XYRPIPBXI8YIFXQDR82UXKHDT03L8JY03398U1I5EMVCGIL9AYUAD9AZN2N582OGQPGG9XD89A2N245SW3N443RNV8H8"
    )
    get_job_output_response: GetJobOutputOutputTypeDef = glacier_apis.get_job_output(
        vaultName="vault1",
        jobId=initiate_job_response["jobId"],
        accountId="123456789012",
    )
    assert (
        get_job_output_response["body"].read()
        == b"ArchiveId,ArchiveDescription,CreationDate,Size,SHA256TreeHash\n098f6bcd4621d373cade4e832627b4f6,,2023-04-11T15:18:41.000Z,4,9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    )


def test_vault1_get_job_output_with_range(glacier_apis: GlacierClient) -> None:
    get_job_output_response: GetJobOutputOutputTypeDef = glacier_apis.get_job_output(
        vaultName="vault1",
        jobId="test-job-id",
        accountId="123456789012",
        range="0-1023",
    )
    assert get_job_output_response["body"].read() == b"test body"
