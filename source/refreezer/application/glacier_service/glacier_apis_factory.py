"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Optional
import boto3
from mypy_boto3_glacier.client import GlacierClient
from refreezer.application.mocking.mock_glacier_apis import MockGlacierAPIs


class GlacierAPIsFactory:
    """
    This class is used to create an instance from either the actual Glacier
    or the mock APIs, depending on the passed parameter 'mock'

    Usage example:
    - For real Glacier APIs
        glacier = GlacierAPIsFactory.create_instance()
        glacier.get_job_output(params)
    - For Mock Glacier APIs
        mockGlacier = GlacierAPIsFactory.create_instance(mock=True)
        mockGlacier.get_job_output(params)
    """

    @staticmethod
    def create_instance(mock: bool = False) -> GlacierClient:
        if mock:
            return MockGlacierAPIs()
        client: GlacierClient = boto3.client("glacier")
        return client
