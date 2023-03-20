"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import pytest


@pytest.fixture
def glacier_job_result() -> typing.Dict[str, typing.Any]:
    return {
        "Action": "InventoryRetrieval",
        "ArchiveId": None,
        "ArchiveSHA256TreeHash": None,
        "ArchiveSizeInBytes": None,
        "Completed": True,
        "CompletionDate": "2023-03-03T21:42:40.684Z",
        "CreationDate": "2023-03-03T17:53:45.420Z",
        "InventoryRetrievalParameters": {
            "EndDate": None,
            "Format": "CSV",
            "Limit": None,
            "Marker": None,
            "StartDate": None,
        },
        "InventorySizeInBytes": 1024,
        "JobDescription": "This is a test",
        "JobId": "KXt2zItqLEKWXWyHk__7sVM8PfNIrdrsdtTLMPsyzXMnIriEK4lzltZgN7erM6_-VLXwOioQapa8EOgKfqTpqeGWuGpk",
        "RetrievalByteRange": None,
        "SHA256TreeHash": None,
        "SNSTopic": "ARN",
        "StatusCode": "Succeeded",
        "StatusMessage": "Succeeded",
        "Tier": None,
        "VaultARN": "ARN",
    }
