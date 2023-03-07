"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest
from typing import Dict, Any

from refreezer.application.model.facilitator import (
    FacilitatorModel,
)


@pytest.fixture
def job_result() -> Dict[str, Any]:
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


def test_get_primary_key_name() -> None:
    job_item = FacilitatorModel("job-123")
    assert job_item.primary_key == "job_id"


def test_check_job_success_status_success(job_result: Dict[str, Any]) -> None:
    job_item = FacilitatorModel("job-123")
    job_item.task_token = "task-123"
    job_item.job_result = job_result
    assert job_item.check_job_success_status() is True


def test_check_job_success_status_failed(job_result: Dict[str, Any]) -> None:
    job_item = FacilitatorModel("job-123")
    job_item.task_token = "task-123"
    job_result["StatusCode"] = "Failed"
    job_item.job_result = job_result
    assert job_item.check_job_success_status() is False


def test_check_if_job_finished(job_result: Dict[str, Any]) -> None:
    job_item = FacilitatorModel("job-123")
    job_item.job_result = job_result
    assert job_item.check_if_job_finished() is True


def test_check_if_job_finished_in_progress(job_result: Dict[str, Any]) -> None:
    job_item = FacilitatorModel("job-123")
    job_result["StatusCode"] = "InProgress"
    job_item.job_result = job_result
    assert job_item.check_if_job_finished() is False
