"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


def trim_inventory_chunk(
    first_chunk: bool, last_chunk: bool, max_record_size: int, chunk: bytes
) -> bytes:
    if not first_chunk:
        # Remove all bytes from the beginning of the chunk array up to
        # (not including) the last occurrence of the newline character \n
        # within the first max_record_size bytes of the chunk array
        chunk = chunk[chunk.rindex(b"\n", 0, max_record_size) :]

    if not last_chunk:
        # Remove all bytes from the end of the chunk array up to and
        # including the last occurrence of the newline character \n
        chunk = chunk[: chunk.rindex(b"\n")]

    return chunk
