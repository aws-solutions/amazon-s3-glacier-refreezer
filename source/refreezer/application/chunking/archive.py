"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import List


def generate_chunk_array(archive_size: int, chunk_size: int) -> List[str]:
    """
    The range to download must be megabyte and treehash aligned
    in order to receive checksum values when downloading.
    https://docs.aws.amazon.com/amazonglacier/latest/dev/checksum-calculations-range.html#tree-hash-algorithm
    """

    if not is_power_of_two(chunk_size):
        raise ValueError(
            "Chunk size should be a power of 2 to be megabyte and treehash aligned."
        )

    chunks = []
    start_index = 0
    end_index = min(archive_size, chunk_size) - 1
    while end_index < archive_size - 1:
        chunks.append(f"{start_index}-{end_index}")
        start_index = end_index + 1
        end_index = min(start_index + chunk_size - 1, archive_size - 1)
    chunks.append(f"{start_index}-{end_index}")
    return chunks


def is_power_of_two(n: int) -> bool:
    return (n != 0) and (n & (n - 1) == 0)
