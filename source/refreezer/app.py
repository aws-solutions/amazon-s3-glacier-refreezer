"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import aws_cdk as cdk

from refreezer.infrastructure.stack import RefreezerStack
from refreezer.pipeline.stack import PipelineStack


def main() -> None:
    app = cdk.App()
    RefreezerStack(app, "refreezer")
    PipelineStack(app, "pipeline")
    app.synth()
