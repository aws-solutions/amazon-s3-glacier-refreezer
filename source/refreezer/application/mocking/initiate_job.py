"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any, TYPE_CHECKING
import boto3
import json

from refreezer.application.glacier_service.glacier_apis_factory import (
    GlacierAPIsFactory,
)

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

    glacier_client = GlacierAPIsFactory.create_instance(True)
    job_response = glacier_client.initiate_job(
        vaultName=vault_name,
        accountId=account_id,
        jobParameters={"Type": "inventory-retrieval"},
    )

    function_params = {
        "account_id": account_id,
        "vault_name": vault_name,
        "sns_topic": sns_topic,
        "job_id": job_response["jobId"],
    }
    client.invoke(
        FunctionName=mock_notify_sns_lambda_arn,
        InvocationType="Event",
        Payload=json.dumps(function_params),
    )

    return {
        "Location": job_response["location"],
        "JobId": job_response["jobId"],
    }
