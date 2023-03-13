"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from refreezer.application.glacier_service.glacier_apis import GlacierAPIs
from refreezer.application.mocking.mock_glacier_apis import MockGlacierAPIs


class GlacierInterface(ABC):
    @abstractmethod
    def get_job_output(self, params: Dict[str, Any]) -> None:
        pass


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
    def create_instance(mock: bool = False) -> GlacierInterface:
        if mock:
            return MockGlacierAPIs()
        return GlacierAPIs()
