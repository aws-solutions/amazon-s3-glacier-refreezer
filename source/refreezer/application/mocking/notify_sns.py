"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING
import os
import boto3
import json
import time

if TYPE_CHECKING:
    from mypy_boto3_sns import SNSClient
else:
    SNSClient = object

NOTIFICATION_DELAY_IN_SEC = 5
INVENTORY_SIZE_IN_BYTES = 2**20 * 100


def notify_sns_job_completion(
    account_id: str, vault_name: str, job_id: str, sns_topic: str
) -> None:
    client: SNSClient = boto3.client("sns")
    time.sleep(NOTIFICATION_DELAY_IN_SEC)
    message = {
        "Action": "InventoryRetrieval",
        "Completed": True,
        "CompletionDate": "2023-01-01T01:01:01.001Z",
        "CreationDate": "2023-01-01T02:02:02.002Z",
        "InventoryRetrievalParameters": {
            "Format": "CSV",
        },
        "InventorySizeInBytes": INVENTORY_SIZE_IN_BYTES,
        "JobDescription": "Mock response from mock lambda",
        "JobId": job_id,
        "SNSTopic": sns_topic,
        "StatusCode": "Succeeded",
        "StatusMessage": "Succeeded",
        "VaultARN": f"arn:aws:glacier:{os.environ['AWS_REGION']}:{account_id}:vaults/{vault_name}",
    }

    client.publish(
        TopicArn=sns_topic,
        Message=json.dumps(message),
        Subject="Notification From Mocking Glacier Service",
    )
