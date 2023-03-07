"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any
from refreezer.application.mocking.initiate_job import (
    generate_inventory_retrieval_response,
)
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def mock_glacier_initiate_job_task_handler(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    LOGGER.info("Mock Glacier initiate job task has been invoked.")
    account_id = event.get("AccountId", "testing_account_id")
    vault_name = event.get("VaultName", "testing_vault_name")

    return generate_inventory_retrieval_response(account_id, vault_name)
