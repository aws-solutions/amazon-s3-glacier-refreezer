"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest
from typing import Dict, Any

from refreezer.application.model.facilitator import (
    FacilitatorModel,
)


def test_get_primary_key_name() -> None:
    job_item = FacilitatorModel("job-123")
    assert job_item.primary_key == "job_id"


def test_check_job_success_status_success(glacier_job_result: Dict[str, Any]) -> None:
    job_item = FacilitatorModel("job-123")
    job_item.task_token = "task-123"
    job_item.job_result = glacier_job_result
    assert job_item.check_job_success_status() is True


def test_check_job_success_status_failed(glacier_job_result: Dict[str, Any]) -> None:
    job_item = FacilitatorModel("job-123")
    job_item.task_token = "task-123"
    glacier_job_result["StatusCode"] = "Failed"
    job_item.job_result = glacier_job_result
    assert job_item.check_job_success_status() is False


def test_check_still_in_progress(glacier_job_result: Dict[str, Any]) -> None:
    job_item = FacilitatorModel("job-123")
    job_item.job_result = glacier_job_result
    assert job_item.check_still_in_progress() is False


def test_check_still_in_progress_in_progress(
    glacier_job_result: Dict[str, Any]
) -> None:
    job_item = FacilitatorModel("job-123")
    glacier_job_result["StatusCode"] = "InProgress"
    job_item.job_result = glacier_job_result
    assert job_item.check_still_in_progress() is True
