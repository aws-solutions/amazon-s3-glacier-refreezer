"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import (
    Aws,
    Stack,
    CfnOutput,
)
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_sns as sns
from aws_cdk import aws_s3 as s3
from aws_cdk import RemovalPolicy
from cdk_nag import NagSuppressions
from constructs import Construct
from typing import Optional


from refreezer.mocking.mock_glacier_stack import MockingParams
from refreezer.infrastructure.async_facilitator import AsyncFacilitator
from refreezer.infrastructure.inventory_retrieval_workflow import (
    InventoryRetrievalWorkflow,
)
from refreezer.infrastructure.initiate_retrieval_workflow import (
    InitiateRetrievalWorkflow,
)

from refreezer.infrastructure.archive_retrieval_workflow import ArchiveRetrievalWorkflow
from refreezer.infrastructure.output_keys import OutputKeys


class RefreezerStack(Stack):
    outputs: dict[str, CfnOutput]

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        mock_params: Optional[MockingParams] = None,
    ) -> None:
        super().__init__(scope, construct_id)

        self.outputs = {}

        async_facilitator_table = dynamodb.Table(
            self,
            "AsyncFacilitatorTable",
            partition_key=dynamodb.Attribute(
                name="job_id", type=dynamodb.AttributeType.STRING
            ),
            point_in_time_recovery=True,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        )

        self.outputs[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME] = CfnOutput(
            self,
            OutputKeys.ASYNC_FACILITATOR_TABLE_NAME,
            value=async_facilitator_table.table_name,
        )

        glacier_retrieval_table = dynamodb.Table(
            self,
            "GlacierObjectRetrieval",
            partition_key=dynamodb.Attribute(
                name="pk", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="sk", type=dynamodb.AttributeType.STRING),
        )

        self.outputs[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME] = CfnOutput(
            self,
            OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME,
            value=glacier_retrieval_table.table_name,
        )

        NagSuppressions.add_resource_suppressions(
            glacier_retrieval_table.node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-DDB3",
                    "reason": "Point In Time Recovery is disabled by default for the table due to cost considerations.  Will make configuration changes to optionally enable PITR in the future.",
                },
            ],
        )

        topic = sns.Topic(self, "AsyncFacilitatorTopic")

        topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                conditions={"Bool": {"aws:SecureTransport": False}},
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                resources=[topic.topic_arn],
            )
        )

        topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["SNS:Publish"],
                effect=iam.Effect.ALLOW,
                resources=[topic.topic_arn],
                principals=[
                    iam.ServicePrincipal("glacier.amazonaws.com"),
                ],
                conditions={"StringEquals": {"AWS:SourceOwner": Aws.ACCOUNT_ID}},
            )
        )

        NagSuppressions.add_resource_suppressions(
            topic.node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-SNS2",
                    "reason": "SNS Topic does not have server-side encryption enabled to be able to receive notifications from Glacier",
                },
            ],
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

        AsyncFacilitator(self, async_facilitator_table, topic, self.outputs)

        InventoryRetrievalWorkflow(
            self,
            async_facilitator_table,
            topic,
            inventory_bucket,
            output_bucket,
            self.outputs,
            mock_params,
        )

        InitiateRetrievalWorkflow(
            self,
            async_facilitator_table,
            glacier_retrieval_table,
            topic,
            inventory_bucket,
            output_bucket,
            self.outputs,
            mock_params,
        )

        ArchiveRetrievalWorkflow(
            self,
            async_facilitator_table,
            glacier_retrieval_table,
            inventory_bucket,
            output_bucket,
            self.outputs,
            mock_params,
        )
