"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import (
    Stack,
    CfnOutput,
)
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_sns as sns
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import RemovalPolicy
from cdk_nag import NagSuppressions
from constructs import Construct
from typing import Optional, Dict, Any


class OutputKeys:
    ASYNC_FACILITATOR_TABLE_NAME = "AsyncFacilitatorTableName"
    ASYNC_FACILITATOR_TOPIC_ARN = "AsyncFacilitatorTopicArn"
    OUTPUT_BUCKET_NAME = "OutputBucketName"
    INVENTORY_BUCKET_NAME = "InventoryBucketName"


class RefreezerStack(Stack):
    outputs: dict[str, CfnOutput]

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        mock_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(scope, construct_id)

        self.outputs = {}

        table = dynamodb.Table(
            self,
            "AsyncFacilitatorTable",
            partition_key=dynamodb.Attribute(
                name="job_id", type=dynamodb.AttributeType.STRING
            ),
            point_in_time_recovery=True,
        )

        self.outputs[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME] = CfnOutput(
            self,
            OutputKeys.ASYNC_FACILITATOR_TABLE_NAME,
            value=table.table_name,
        )

        sns_default_key = kms.Alias.from_alias_name(
            self, "DefaultKeySNS", "alias/aws/sns"
        )

        topic = sns.Topic(
            self,
            "AsyncFacilitatorTopic",
            master_key=sns_default_key,
        )

        topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                conditions={"Bool": {"aws:SecureTransport": False}},
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                resources=[topic.topic_arn],
            )
        )

        self.outputs[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN] = CfnOutput(
            self,
            OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN,
            value=topic.topic_arn,
        )

        # Bucket to store the restored vault.
        # TODO This bucket will be made configurable in a future task.
        output_bucket = s3.Bucket(
            self,
            "OutputBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.outputs[OutputKeys.OUTPUT_BUCKET_NAME] = CfnOutput(
            self,
            OutputKeys.OUTPUT_BUCKET_NAME,
            value=output_bucket.bucket_name,
        )

        NagSuppressions.add_resource_suppressions(
            output_bucket,
            [
                {
                    "id": "AwsSolutions-S1",
                    "reason": "Output Bucket has server access logs disabled and will be addressed later.",
                }
            ],
        )

        # Bucket to store the inventory and the Glue output after it's sorted.
        # TODO This bucket will be made configurable in a future task.
        inventory_bucket = s3.Bucket(
            self,
            "InventoryBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        self.outputs[OutputKeys.INVENTORY_BUCKET_NAME] = CfnOutput(
            self,
            OutputKeys.INVENTORY_BUCKET_NAME,
            value=inventory_bucket.bucket_name,
        )

        NagSuppressions.add_resource_suppressions(
            inventory_bucket,
            [
                {
                    "id": "AwsSolutions-S1",
                    "reason": "Inventory Bucket has server access logs disabled and will be addressed later.",
                }
            ],
        )

        vault_name = "test-vault"
        state_json = {
            "Type": "Task",
            "Parameters": {
                "AccountId": Stack.of(self).account,
                "JobParameters": {
                    "Type": "inventory-retrieval",
                    "Format": "CSV",
                    "Description": "Inventory retrieval job",
                    "SnsTopic": topic.topic_arn,
                },
                "VaultName": vault_name,
            },
            "Resource": "arn:aws:states:::aws-sdk:glacier:initiateJob",
        }
        get_inventory_initiate_job = sfn.CustomState(
            scope, "GetInventoryInitiateJob", state_json=state_json
        )
        if mock_params is not None:
            # TODO replace each Glacier initiate job task by the mock task
            mock_glacier_initiate_job_task = mock_params[
                "mock_glacier_initiate_job_task"
            ]
            get_inventory_initiate_job = mock_glacier_initiate_job_task

        state_machine = sfn.StateMachine(
            self, "StateMachine", definition=get_inventory_initiate_job
        )

        NagSuppressions.add_resource_suppressions(
            state_machine,
            [
                {
                    "id": "AwsSolutions-SF1",
                    "reason": "The Step Function does not log ALL events to CloudWatch and will be addressed later.",
                },
                {
                    "id": "AwsSolutions-SF2",
                    "reason": "The Step Function does not have X-Ray tracing enabled and will be addressed later.",
                },
            ],
        )
