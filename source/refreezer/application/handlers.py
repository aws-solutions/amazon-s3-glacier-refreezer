"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any
import json
from refreezer.application.mocking.initiate_job import (
    generate_inventory_retrieval_response,
)

vault_name = "test-vault-01"


def mock_glacier_initiate_job_task_handler(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    return generate_inventory_retrieval_response()
