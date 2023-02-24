"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_lambda as lambda_


class MockGlacierStack(Stack):
    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)
        mock_glacier_initiate_job_lambda = lambda_.Function(
            self,
            "MockGalcierService",
            handler="refreezer.application.handlers.mock_glacier_initiate_job_task_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to mock Glacier service initiate job task for integration tests.",
        )
