"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import boto3
import logging
from typing import List, Dict, TYPE_CHECKING, Optional, Any

from refreezer.application.facilitator.processor import sns_handler, dynamoDb_handler
from refreezer.application.chunking.inventory import generate_chunk_array
from refreezer.application.archive_transfer.facilitator import (
    ArchiveTransferFacilitator,
)


if TYPE_CHECKING:
    from mypy_boto3_stepfunctions.client import SFNClient
else:
    SFNClient = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def async_facilitator_handler(
    event: Dict[str, List[Any]], _: Optional[Dict[str, Any]]
) -> None:
    sfn_client: SFNClient = boto3.client("stepfunctions")
    for record in event["Records"]:
        if record.get("EventSource") == "aws:sns":
            sns_handler(record, sfn_client)
        else:
            dynamoDb_handler(record, sfn_client)


def chunk_retrieval_lambda_handler(
    event: Dict[str, Any], _context: Any
) -> Dict[str, Any]:
    logger.info("Chunk retrieval lambda has been invoked.")
    facilitator = ArchiveTransferFacilitator(
        event["JobId"],
        event["VaultName"],
        event["StartByte"],
        event["EndByte"],
        event["ChunkSize"],
        event["DestinationBucket"],
        event["ArchiveKey"],
        event["ArchiveId"],
        event.get("UploadId"),
        event.get("PartNumber"),
    )
    facilitator.transfer_archive()
    return {"body": "Chunk retrieval lambda has completed."}


def chunk_validation_lambda_handler(
    event: Dict[str, Any], _context: Any
) -> Dict[str, Any]:
    logger.info("Chunk validation lambda has been invoked.")
    return {"body": "Chunk validation lambda has completed."}


def inventory_chunk_lambda_handler(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    logger.info("Inventory chunk lambda has been invoked.")

    inventory_size = event["InventorySize"]
    maximum_inventory_record_size = event["MaximumInventoryRecordSize"]
    chunk_size = event["ChunkSize"]

    chunks = generate_chunk_array(
        inventory_size, maximum_inventory_record_size, chunk_size
    )

    return {"body": chunks}


def inventory_chunk_download_lambda_handler(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    logger.info("Chunk retrieval lambda has been invoked.")

    return {"InventoryRetrieved": "TRUE"}
