"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
from typing import Dict, Any

logger = logging.getLogger()


class StatusCode:
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    IN_PROGRESS = "InProgress"


class FacilitatorModel:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.job_result: Dict[str, Any] = {}
        self.task_token = ""
        self.finish_timestamp = ""
        self.start_timestamp = ""

    @property
    def primary_key(self) -> str:
        return "job_id"

    def check_job_success_status(self) -> bool:
        result: bool = self.job_result["StatusCode"] == StatusCode.SUCCEEDED
        result_str = "succeeded" if result else "has not succeeded"
        getattr(logger, "debug" if result else "error")(
            f"The job with job-id {self.job_id} {result_str}"
        )
        return result

    def check_still_in_progress(self) -> bool:
        if self.job_result["StatusCode"] == StatusCode.IN_PROGRESS:
            logger.info(f"The job with job-id {self.job_id} is still in progress")
            return True
        return False
