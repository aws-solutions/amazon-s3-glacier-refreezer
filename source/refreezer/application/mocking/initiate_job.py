"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any, TYPE_CHECKING
import random
import string
import boto3
import json

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = object


def generate_inventory_retrieval_response(
    account_id: str,
    vault_name: str,
    sns_topic: str,
    mock_notify_sns_lambda_arn: str,
) -> Dict[str, Any]:
    client: LambdaClient = boto3.client("lambda")
    job_id = "".join(random.choices(string.ascii_letters + string.digits, k=92))

    function_params = {
        "account_id": account_id,
        "vault_name": vault_name,
        "sns_topic": sns_topic,
        "job_id": job_id,
    }
    client.invoke(
        FunctionName=mock_notify_sns_lambda_arn,
        InvocationType="Event",
        Payload=json.dumps(function_params),
    )

    return {
        "Location": f"/{account_id}/vaults/{vault_name}/jobs/{job_id}",
        "JobId": job_id,
    }
