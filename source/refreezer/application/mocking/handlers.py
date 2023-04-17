"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any
from refreezer.application.mocking.initiate_job import (
    generate_inventory_retrieval_response,
)
from refreezer.application.mocking.notify_sns import notify_sns_job_completion
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def mock_glacier_initiate_job_task_handler(
    event: Dict[str, Any], _context: Any
) -> Dict[str, Any]:
    logger.info("Mock Glacier initiate job task has been invoked.")
    mock_notify_sns_lambda_arn = os.environ["MOCK_NOTIFY_SNS_LAMBDA_ARN"]
    account_id = event.get("account_id", "testing_account_id")
    vault_name = event.get("vault_name", "testing_vault_name")
    sns_topic = event.get("sns_topic", "testing_sns_topic")

    return generate_inventory_retrieval_response(
        account_id, vault_name, sns_topic, mock_notify_sns_lambda_arn
    )


def mock_notify_sns_handler(event: Dict[str, Any], _context: Any) -> None:
    account_id = event.get("account_id", "testing_account_id")
    vault_name = event.get("vault_name", "testing_vault_name")
    sns_topic = event.get("sns_topic", "testing_sns_topic")
    job_id = event.get("job_id", "testing_job_id")
    notify_sns_job_completion(account_id, vault_name, job_id, sns_topic)
