"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest
from typing import List

from refreezer.application.chunking.archive import generate_chunk_array


def test_generate_chunk_array_size_is_not_power_of_two() -> None:
    with pytest.raises(Exception):
        generate_chunk_array(100, 10)


@pytest.mark.parametrize(
    "archive_size, chunk_size, expected_chunks",
    [
        (384, 128, ["0-127", "128-255", "256-383"]),
        (100, 128, ["0-99"]),
        (128, 128, ["0-127"]),
        (512, 128, ["0-127", "128-255", "256-383", "384-511"]),
        (547, 128, ["0-127", "128-255", "256-383", "384-511", "512-546"]),
    ],
)
def test_generate_chunk_array(
    archive_size: int,
    chunk_size: int,
    expected_chunks: List[str],
) -> None:
    assert expected_chunks == generate_chunk_array(archive_size, chunk_size)
