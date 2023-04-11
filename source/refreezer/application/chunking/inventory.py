"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import List

from refreezer.application.util.exceptions import ChunkSizeTooSmall


def generate_chunk_array(
    inventory_size: int, maximum_inventory_record_size: int, chunk_size: int
) -> List[str]:
    """
    Generate byte range offsets to retrieve the inventory.
    Byte ranges overlap by the maximum inventory record size.
    When retrieving the inventory, newline delimiter will be used to trim data to only include complete records.
    """

    chunks = []
    start_index = 0
    end_index = min(inventory_size, chunk_size) - 1

    if chunk_size < maximum_inventory_record_size:
        raise ChunkSizeTooSmall(chunk_size, maximum_inventory_record_size)

    while end_index < inventory_size - 1:
        chunks.append(f"{start_index}-{end_index}")
        start_index = end_index - maximum_inventory_record_size + 1
        end_index = min(start_index + chunk_size - 1, inventory_size - 1)

    chunks.append(f"{start_index}-{end_index}")
    return chunks
