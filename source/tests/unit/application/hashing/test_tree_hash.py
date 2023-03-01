"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import hashlib

from refreezer.application.hashing.tree_hash import TreeHash


def test_single_chunk() -> None:
    data = b"abc" * 100000
    expected_hash = hashlib.sha256(data).digest()
    th = TreeHash()
    th.update(data)
    assert th.digest() == expected_hash


def test_multiple_chunks() -> None:
    data = b"ab" * 150000
    chunk_size = 100001
    hashes = [
        hashlib.sha256(data[i : i + chunk_size]).digest()
        for i in range(0, len(data), chunk_size)
    ]
    first_hash = hashlib.sha256(b"".join(hashes[:2])).digest()
    expected_hash = hashlib.sha256(b"".join([first_hash, hashes[2]])).digest()
    th = TreeHash(chunk_size=chunk_size)
    th.update(data)
    assert th.digest() == expected_hash


def test_empty_data() -> None:
    data = b""
    th = TreeHash()
    th.update(data)
    assert th.digest() == b""


def test_large_data() -> None:
    data = b"0" * (2**30)
    chunk_size = 2**20
    expected_hash = hashlib.sha256(data[0:chunk_size]).digest()
    for _ in range(10):  # Since initial hashes are all identical, we can reuse the hash
        expected_hash = hashlib.sha256(
            b"".join([expected_hash, expected_hash])
        ).digest()
    th = TreeHash(chunk_size=chunk_size)
    th.update(data)
    assert th.digest() == expected_hash


def test_precomputed_hash() -> None:
    data = b"abc" * 1234567
    expected_hash = "5a245eddef1227623912286af0a6dd6f1a3cd67ad3d768c1490c2f8a0fafeca9"  # Calculated by uploading identical data to Glacier
    th = TreeHash()
    th.update(data)
    assert th.digest().hex() == expected_hash
