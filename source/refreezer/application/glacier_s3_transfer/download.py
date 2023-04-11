"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING, Optional
from refreezer.application.util.exceptions import AccessViolation

if TYPE_CHECKING:
    from mypy_boto3_glacier.client import GlacierClient
    from mypy_boto3_glacier.type_defs import GetJobOutputOutputTypeDef
else:
    GlacierClient = object
    GetJobOutputOutputTypeDef = object


class GlacierDownload:
    def __init__(
        self,
        glacier_client: GlacierClient,
        job_id: str,
        vault_name: str,
        byte_range: str,
    ) -> None:
        self.params = {
            "jobId": job_id,
            "range": f"bytes={byte_range}",
            "vaultName": vault_name,
        }
        self.response: GetJobOutputOutputTypeDef = glacier_client.get_job_output(
            **self.params
        )
        self.accessed = False

    def read(self) -> bytes:
        if self.accessed:
            raise AccessViolation()
        self.accessed = True
        return self.response["body"].read()

    def checksum(self) -> Optional[str]:
        return self.response.get("checksum")
