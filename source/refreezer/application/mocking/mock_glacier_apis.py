"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
from typing import List, Dict, Callable, TYPE_CHECKING
from datetime import timedelta
from unittest.mock import patch
import boto3
from moto import mock_glacier  # type: ignore
import requests  # type: ignore

if TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import (
        GetJobOutputOutputTypeDef,
        InitiateJobOutputTypeDef,
        JobParametersTypeDef,
    )
else:
    GlacierClient = object
    GetJobOutputOutputTypeDef = object
    InitiateJobOutputTypeDef = object
    JobParametersTypeDef = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class MockGlacierAPIs(GlacierClient):
    def __init__(self, instructions: List[str]) -> None:
        logger.info("Creating MockGlacierAPIs.")
        with mock_glacier():
            self.glacier: GlacierClient = boto3.client("glacier")
        self.glacier_job_mapping: Dict[str, Dict[str, InitiateJobOutputTypeDef]] = {}
        self.inventory: List[str] = []
        requests.post("http://motoapi.amazonaws.com/moto-api/seed?a=42")
        self._initialize_vault(instructions)

    def get_job_output(
        self, *, vaultName: str, jobId: str, accountId: str = "-", range: str = ""
    ) -> GetJobOutputOutputTypeDef:
        return self.glacier.get_job_output(
            vaultName=vaultName, jobId=jobId, accountId=accountId, range=range
        )

    def initiate_job(
        self,
        *,
        vaultName: str,
        accountId: str = "-",
        jobParameters: JobParametersTypeDef = {},
    ) -> InitiateJobOutputTypeDef:
        return self.glacier_job_mapping[vaultName][
            jobParameters.get("ArchiveId", vaultName)
        ]

    def _initialize_vault(self, instructions: List[str]) -> None:
        logger.info("Setting up MockGlacierAPIs.")
        action_map: Dict[str, Callable[..., None]] = {
            "CREATE_VAULT": self._create_vault,
            "UPLOAD_ARCHIVE": self._upload_archive,
            "INITIATE_INVENTORY_RETRIEVAL": self._initiate_inventory_retrieval,
            "INITIATE_ARCHIVE_RETRIEVAL": self._initate_archive_retrieval,
        }
        with patch("datetime.timedelta", return_value=timedelta(seconds=0)):
            for instruction in instructions:
                action, *args = instruction.split()
                action_map[action](*args)

    def _create_vault(self, vault_name: str) -> None:
        self.glacier_job_mapping[vault_name] = {}
        self.glacier.create_vault(vaultName=vault_name)

    def _upload_archive(self, vault_name: str, body: str) -> None:
        response = self.glacier.upload_archive(
            vaultName=vault_name, body=bytes(body, "utf-8")
        )
        self.inventory.append(response["archiveId"])

    def _initiate_inventory_retrieval(self, vault_name: str) -> None:
        response = self.glacier.initiate_job(
            vaultName=vault_name, jobParameters={"Type": "inventory-retrieval"}
        )
        self.glacier_job_mapping[vault_name][vault_name] = response

    def _initate_archive_retrieval(self, vault_name: str, archive_index: str) -> None:
        archive_id = self.inventory[int(archive_index)]
        response = self.glacier.initiate_job(
            vaultName=vault_name,
            jobParameters={"Type": "archive-retrieval", "ArchiveId": archive_id},
        )
        self.glacier_job_mapping[vault_name][archive_id] = response
