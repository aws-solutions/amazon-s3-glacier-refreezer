"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any
import random
import string


def generate_inventory_retrieval_response(
    account_id: str, vault_name: str
) -> Dict[str, Any]:
    job_id = "".join(random.choices(string.ascii_letters + string.digits, k=92))
    return {
        "Location": f"/{account_id}/vaults/{vault_name}/jobs/{job_id}",
        "JobId": job_id,
    }
