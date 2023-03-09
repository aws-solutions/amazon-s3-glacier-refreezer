"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions_tasks as tasks


class MockingParams:
    mock_glacier_initiate_job_task: tasks.LambdaInvoke
    mock_glacier_initiate_job_lambda_arn: str


class MockGlacierStack(Stack):
    params: MockingParams

    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)
        mock_glacier_initiate_job_lambda = lambda_.Function(
            self,
            "MockGlacierService",
            handler="refreezer.application.mocking.handlers.mock_glacier_initiate_job_task_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to mock Glacier service initiate job task for integration tests.",
        )

        mock_glacier_initiate_job_task = tasks.LambdaInvoke(
            scope,
            "MockGlacierInitiateJobTask",
            lambda_function=mock_glacier_initiate_job_lambda,
            payload_response_only=True,
        )

        self.params = MockingParams()
        self.params.mock_glacier_initiate_job_task = mock_glacier_initiate_job_task
        self.params.mock_glacier_initiate_job_lambda_arn = (
            mock_glacier_initiate_job_lambda.function_arn
        )
