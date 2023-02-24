"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any
import json


def mock_glacier_initiate_job_task_handler(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    return {
        "body": json.dumps("Mock Glacier InitiateJob task lambda has been invoked.!")
    }
