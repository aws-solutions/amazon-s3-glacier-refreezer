"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import boto3
from typing import Any


class GlacierDownload:
    def __init__(
        self,
        job_id: str,
        vault_name: str,
        start_byte: int,
        end_byte: int,
        chunk_size: int,
    ) -> None:
        self.params = {
            "jobId": job_id,
            "range": f"bytes={start_byte}-{end_byte}",
            "vaultName": vault_name,
        }
        self.glacier = boto3.client("glacier")
        self.chunk_size = chunk_size
        self.response = self.glacier.get_job_output(**self.params)

    def iter(self) -> Any:
        return self.response["body"].iter_chunks(chunk_size=self.chunk_size)

    def checksum(self) -> Any:
        print(self.response)
        return self.response.get("checksum")
