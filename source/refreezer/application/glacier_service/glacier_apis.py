"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging
from typing import Dict, Any

from refreezer.application.glacier_service.glacier_apis_factory import GlacierInterface

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class GlacierAPIs(GlacierInterface):
    def get_job_output(self, params: Dict[str, Any]) -> None:
        logger.info("Calling get_job_output in GlacierAPIs.")
        # TODO use actual Glacier get_job_output API
        pass
