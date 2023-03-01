"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Dict, Any
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def chunk_retrieval_lambda_handler(
    event: Dict[str, Any], context: Any
) -> Dict[str, Any]:
    LOGGER.info("Chunk retrieval lambda has been invoked.")

    return {"body": ""}
