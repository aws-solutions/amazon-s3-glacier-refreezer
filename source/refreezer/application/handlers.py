"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any
import logging

from refreezer.application.chunking.inventory import generate_chunk_array

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def chunk_retrieval_lambda_handler(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    LOGGER.info("Chunk retrieval lambda has been invoked.")

    return {"body": ""}


def inventory_chunk_lambda_handler(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    LOGGER.info("Inventory chunk lambda has been invoked.")

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
    LOGGER.info("Chunk retrieval lambda has been invoked.")

    return {"InventoryRetrieved": "TRUE"}
