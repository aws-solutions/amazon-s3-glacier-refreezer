"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
from typing import Any, Dict, TYPE_CHECKING
from botocore.response import StreamingBody
import io
from refreezer.application.mocking.mock_glacier_data import MOCK_DATA

if TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import (
        JobParametersTypeDef,
    )
else:
    GlacierClient = object
    JobParametersTypeDef = object

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class MockGlacierAPIs(GlacierClient):
    def __init__(self) -> None:
        self.output_mapping: Dict[str, Any] = MOCK_DATA

    def get_job_output(
        self, *, vaultName: str, jobId: str, accountId: str = "-", range: str = ""
    ) -> Any:
        output = self.output_mapping[vaultName]["get-job-output"][jobId]
        if range != "":
            output = output[range]
        body = output["body"]
        output["body"] = StreamingBody(io.BytesIO(bytes(body, "utf-8")), len(body))
        return output

    def initiate_job(
        self,
        *,
        vaultName: str,
        accountId: str = "-",
        jobParameters: JobParametersTypeDef = {},
    ) -> Any:
        access_string = jobParameters["Type"]
        if archive_id := jobParameters.get("ArchiveId"):
            access_string = f"{access_string}:{archive_id}"
        return self.output_mapping[vaultName]["initiate-job"][access_string]
