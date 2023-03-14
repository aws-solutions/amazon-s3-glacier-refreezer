"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import hashlib


class S3Hash:
    def __init__(self) -> None:
        self.hashes: list[bytes] = []

    def include(self, hash: bytes) -> None:
        self.hashes.append(hash)

    def _concat(self) -> bytes:
        return b"".join(self.hashes)

    def digest(self) -> bytes:
        return hashlib.sha256(self._concat()).digest()

    @classmethod
    def hash(cls, chunk: bytes) -> bytes:
        return hashlib.sha256(chunk).digest()
