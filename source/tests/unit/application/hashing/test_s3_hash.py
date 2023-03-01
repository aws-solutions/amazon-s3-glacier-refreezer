"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import hashlib

from refreezer.application.hashing.s3_hash import S3Hash


def test_hash() -> None:
    data = b"abc" * 100000
    expected_hash = hashlib.sha256(data).digest()
    assert S3Hash.hash(data) == expected_hash


def test_empty_data() -> None:
    data = b""
    expected_hash = hashlib.sha256(data).digest()
    assert S3Hash.hash(data) == expected_hash


def test_multiple_updates() -> None:
    data = b"abc" * 200000
    chunk_size = 100000
    expected_hashes = [
        hashlib.sha256(data[i : i + chunk_size]).digest()
        for i in range(0, len(data), chunk_size)
    ]
    expected_hash = b"".join(expected_hashes)
    s3h = S3Hash()
    for i in range(0, len(data), chunk_size):
        s3h.include(S3Hash.hash(data[i : i + chunk_size]))
    assert s3h._concat() == expected_hash


def test_multiple_includes() -> None:
    hashes = [b"test", b"test2", b"test3"]
    expected_concat = b"".join(hashes)
    s3h = S3Hash()
    for hash in hashes:
        s3h.include(hash)
    assert s3h._concat() == expected_concat


def test_single_digest() -> None:
    data = b"abc" * 100000
    expected_hash = hashlib.sha256(data).digest()
    s3h = S3Hash()
    s3h.include(S3Hash.hash(data))
    assert s3h.digest() == hashlib.sha256(expected_hash).digest()


def test_multiple_digest() -> None:
    data = b"abc" * 200000
    chunk_size = 100000
    expected_hashes = [
        hashlib.sha256(data[i : i + chunk_size]).digest()
        for i in range(0, len(data), chunk_size)
    ]
    expected_hash = b"".join(expected_hashes)
    s3h = S3Hash()
    for i in range(0, len(data), chunk_size):
        s3h.include(S3Hash.hash(data[i : i + chunk_size]))
    assert s3h.digest() == hashlib.sha256(expected_hash).digest()


def test_concat_empty_hashes() -> None:
    s3h = S3Hash()
    assert s3h._concat() == b""


def test_concat_one_hash() -> None:
    data = b"abc" * 100000
    expected_hash = hashlib.sha256(data).digest()
    s3h = S3Hash()
    s3h.include(S3Hash.hash(data))
    assert s3h._concat() == expected_hash


def test_concat_multiple_hashes() -> None:
    data1 = b"abc" * 100000
    data2 = b"xyz" * 50000
    expected_hash = b"".join(
        [hashlib.sha256(data1).digest(), hashlib.sha256(data2).digest()]
    )
    s3h = S3Hash()
    s3h.include(S3Hash.hash(data1))
    s3h.include(S3Hash.hash(data2))
    assert s3h._concat() == expected_hash
