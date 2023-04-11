"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import re
import pytest

from refreezer.application.chunking.inventory import generate_chunk_array
from refreezer.application.util.exceptions import ChunkSizeTooSmall


def test_generate_chunk_array_correct_chunks_last_chunk_same_size() -> None:
    result = generate_chunk_array(
        inventory_size=1400, maximum_inventory_record_size=200, chunk_size=500
    )
    assert result == ["0-499", "300-799", "600-1099", "900-1399"]


def test_generate_chunk_array_correct_chunks_last_chunk_smaller_size() -> None:
    result = generate_chunk_array(
        inventory_size=1600, maximum_inventory_record_size=200, chunk_size=500
    )
    assert result == ["0-499", "300-799", "600-1099", "900-1399", "1200-1599"]


def test_generate_chunk_array_chunk_smaller_than_record_size() -> None:
    with pytest.raises(ChunkSizeTooSmall):
        generate_chunk_array(
            inventory_size=5000, maximum_inventory_record_size=500, chunk_size=200
        )


def test_generate_chunk_array_one_chunk() -> None:
    result = generate_chunk_array(
        inventory_size=500, maximum_inventory_record_size=200, chunk_size=1000
    )
    assert result == ["0-499"]


def test_generate_chunk_array_correct_size() -> None:
    inventory_size = 2**30 * 10
    maximum_inventory_record_size = 2**10 * 2
    chunk_size = 2**20 * 10

    chunk_array_size, last_chunk_size = divmod(
        inventory_size, chunk_size - maximum_inventory_record_size
    )
    last_chunk_entry = 1 if last_chunk_size - maximum_inventory_record_size > 0 else 0
    chunk_array_size = chunk_array_size + last_chunk_entry

    result = generate_chunk_array(
        inventory_size=inventory_size,
        maximum_inventory_record_size=maximum_inventory_record_size,
        chunk_size=chunk_size,
    )
    assert chunk_array_size == len(result)


def test_generate_chunk_array_correct_entry_format() -> None:
    result = generate_chunk_array(
        inventory_size=5000, maximum_inventory_record_size=500, chunk_size=1000
    )
    assert re.match(r"^\d+-\d+$", result[0])
