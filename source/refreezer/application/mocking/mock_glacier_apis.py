"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
from typing import Dict, Any

from refreezer.application.glacier_service.glacier_apis_factory import GlacierInterface

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class MockGlacierAPIs(GlacierInterface):
    def get_job_output(self, params: Dict[str, Any]) -> None:
        logger.info("Calling get_job_output in MockGlacierAPIs.")
        # TODO mock Glacier get_job_output API
        pass
