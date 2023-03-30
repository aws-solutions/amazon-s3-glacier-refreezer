"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import boto3
import logging
from typing import List, Dict, TYPE_CHECKING, Optional, Any

from refreezer.application.facilitator.processor import sns_handler, dynamoDb_handler
from refreezer.application.chunking.inventory import generate_chunk_array
from refreezer.application.model import events
from refreezer.application.glacier_s3_transfer.facilitator import (
    GlacierToS3Facilitator,
)


if TYPE_CHECKING:
    from mypy_boto3_stepfunctions.client import SFNClient
    from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSEvent
else:
    SFNClient = object
    SQSEvent = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def async_facilitator_handler(event: SQSEvent, _: Optional[Dict[str, Any]]) -> None:
    sfn_client: SFNClient = boto3.client("stepfunctions")
    for record in event["Records"]:
        if record.get("EventSource") == "aws:sns":
            sns_handler(record, sfn_client)
        else:
            dynamoDb_handler(record, sfn_client)


def chunk_retrieval_lambda_handler(
    event: events.ArchiveRetrieval, _context: Any
) -> Dict[str, Any]:
    logger.info("Chunk retrieval lambda has been invoked.")

    facilitator = GlacierToS3Facilitator(
        event["JobId"],
        event["VaultName"],
        event["StartByte"],
        event["EndByte"],
        event["ArchiveId"],
        event["S3DestinationBucket"],
        event["S3DestinationKey"],
        event["UploadId"],
        event["PartNumber"],
    )

    facilitator.transfer()
    return {"body": "Chunk retrieval lambda has completed."}


def chunk_validation_lambda_handler(
    event: events.ChunkValidation, _context: Any
) -> Dict[str, Any]:
    logger.info("Chunk validation lambda has been invoked.")
    return {"body": "Chunk validation lambda has completed."}


def inventory_chunk_lambda_handler(
    event: events.InventoryChunk, _context: Any
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
    event: events.GlacierRetrieval, _context: Any
) -> Dict[str, Any]:
    logger.info("Chunk retrieval lambda has been invoked.")

    return {"InventoryRetrieved": "TRUE"}


def archive_chunk_lambda_handler(
    event: events.ArchiveChunk, _context: Any
) -> Dict[str, Any]:
    logger.info("Archive chunk lambda has been invoked.")

    archive_size = event["ArchiveSize"]
    archive_chunk_size = event["ArchiveChunkSize"]

    # TODO: This is a temporary (testing) solution to get the chunking working. This will be replaced with a proper chunking solution in a future Asana task.
    archive_chunks = {
        "ArchiveChunks": [
            {
                "StartByte": 0,
                "EndByte": archive_chunk_size,
                "ChunkSize": archive_chunk_size,
            }
        ]
    }

    return {"body": archive_chunks}
