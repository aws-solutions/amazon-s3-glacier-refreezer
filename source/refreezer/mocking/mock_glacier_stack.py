"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_lambda as lambda_
from aws_cdk import Duration
from aws_cdk import aws_stepfunctions_tasks as tasks


class MockingParams:
    mock_glacier_inventory_initiate_job_task: tasks.LambdaInvoke
    mock_glacier_archive_initiate_job_task: tasks.LambdaInvoke
    mock_glacier_initiate_job_lambda_arn: str
    mock_notify_sns_lambda_role_arn: str


class MockGlacierStack(Stack):
    params: MockingParams

    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)
        mock_notify_sns_lambda = lambda_.Function(
            self,
            "MockNotifySns",
            handler="refreezer.application.mocking.handlers.mock_notify_sns_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to mock notifying SNS job completion.",
            timeout=Duration.seconds(10),
        )

        mock_glacier_initiate_job_lambda = lambda_.Function(
            self,
            "MockGlacierService",
            handler="refreezer.application.mocking.handlers.mock_glacier_initiate_job_task_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to mock Glacier service initiate job task for integration tests.",
            environment={
                "MOCK_NOTIFY_SNS_LAMBDA_ARN": mock_notify_sns_lambda.function_arn,
            },
        )

        mock_notify_sns_lambda.grant_invoke(mock_glacier_initiate_job_lambda)

        mock_glacier_inventory_initiate_job_task = tasks.LambdaInvoke(
            scope,
            "MockGlacierInventoryInitiateJobTask",
            lambda_function=mock_glacier_initiate_job_lambda,
            result_path="$.initiate_job_result",
            payload_response_only=True,
        )

        mock_glacier_archive_initiate_job_task = tasks.LambdaInvoke(
            scope,
            "MockGlacierArchiveInitiateJobTask",
            lambda_function=mock_glacier_initiate_job_lambda,
            result_path="$.initiate_job_result",
            payload_response_only=True,
        )

        self.params = MockingParams()
        self.params.mock_glacier_inventory_initiate_job_task = (
            mock_glacier_inventory_initiate_job_task
        )
        self.params.mock_glacier_archive_initiate_job_task = (
            mock_glacier_archive_initiate_job_task
        )
        self.params.mock_glacier_initiate_job_lambda_arn = (
            mock_glacier_initiate_job_lambda.function_arn
        )
        assert mock_notify_sns_lambda.role is not None
        self.params.mock_notify_sns_lambda_role_arn = (
            mock_notify_sns_lambda.role.role_arn
        )
