"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Stack
from constructs import Construct
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions_tasks as tasks
from aws_cdk import aws_s3 as s3
from aws_cdk import RemovalPolicy
from cdk_nag import NagSuppressions
from typing import Dict, Any


class MockGlacierStack(Stack):
    params: Dict[str, Any]

    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)
        glacier_vault = s3.Bucket(
            self,
            "MockGlacierVault",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        NagSuppressions.add_resource_suppressions(
            glacier_vault,
            [
                {
                    "id": "AwsSolutions-S1",
                    "reason": "Mock Glacier Vault Bucket has server access logs disabled and will be addressed later.",
                }
            ],
        )

        mock_glacier_initiate_job_lambda = lambda_.Function(
            self,
            "MockGalcierService",
            handler="refreezer.application.handlers.mock_glacier_initiate_job_task_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to mock Glacier service initiate job task for integration tests.",
            environment={
                "mock_glacier_vault_bucket_name": glacier_vault.bucket_name,
                "vault_name": "test-vault-01",
                "max_file_size_in_mb": "5",
                "num_archives": "100",
            },
        )

        glacier_vault.grant_read_write(mock_glacier_initiate_job_lambda)

        mock_glacier_initiate_job_task = tasks.LambdaInvoke(
            scope,
            "MockGlacierInitiateJobTask",
            lambda_function=mock_glacier_initiate_job_lambda,
            payload_response_only=True,
        )

        self.params = {
            "mock_glacier_initiate_job_task": mock_glacier_initiate_job_task,
            "mock_glacier_initiate_job_lambda_arn": mock_glacier_initiate_job_lambda.function_arn,
        }
