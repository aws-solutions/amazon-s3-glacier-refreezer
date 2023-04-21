"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import typing
import re
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
        accountId="",
        jobParameters={"Type": "inventory-retrieval"},
    )
    assert (
        initiate_job_response["location"]
        == "//vaults/vault1/jobs/IEQH524YNG5BY1A2ROGUBBB8AYN1B7O259OWOO3SB09GLSHV616MTS56ZC4PZ0LX9XF26GK7ZX5B4CTZKK6OAM89OZ6W"
    )
    assert (
        initiate_job_response["jobId"]
        == "IEQH524YNG5BY1A2ROGUBBB8AYN1B7O259OWOO3SB09GLSHV616MTS56ZC4PZ0LX9XF26GK7ZX5B4CTZKK6OAM89OZ6W"
    )
    get_job_output_response: GetJobOutputOutputTypeDef = glacier_apis.get_job_output(
        vaultName="vault1",
        jobId=initiate_job_response["jobId"],
        accountId="",
    )
    pattern = rb"^ArchiveId,ArchiveDescription,CreationDate,Size,SHA256TreeHash\r\ncf2e306ff9a72790b152fb4af93a1a1d,test.txt,\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z,8,b9f9644670e5fcd37a4c54a478d636fc37c41282d161e3e50cb3fb962aa04285\r\n$"
    assert re.match(pattern, get_job_output_response["body"].read()) is not None


def test_vault1_get_job_output_with_range(glacier_apis: GlacierClient) -> None:
    get_job_output_response: GetJobOutputOutputTypeDef = glacier_apis.get_job_output(
        vaultName="vault1",
        jobId="W3R9AY6I79N1D4X9M605W0WA88V3BOL9LF9QCEFB2ARPRHLWSEKKQ7KRS3U54HBTYV0MQGQ6N1BOBZJCK2618O72O7BZ",
        accountId="",
        range="bytes=3-5",
    )
    assert get_job_output_response["body"].read() == b"TBO"
