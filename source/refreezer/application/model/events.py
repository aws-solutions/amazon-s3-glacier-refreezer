"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TypedDict


class GlacierRetrieval(TypedDict):
    JobId: str
    VaultName: str
    ByteRange: str
    S3DestinationBucket: str
    S3DestinationKey: str
    UploadId: str
    PartNumber: int


class ArchiveRetrieval(GlacierRetrieval):
    ArchiveId: str


class ChunkValidation(TypedDict):
    ToBeAdded: str


class InventoryChunk(TypedDict):
    InventorySize: int
    MaximumInventoryRecordSize: int
    ChunkSize: int


class ArchiveChunk(TypedDict):
    ArchiveSize: int
    ArchiveChunkSize: int


class ValidateMultipartUpload(TypedDict):
    ToBeAdded: str
