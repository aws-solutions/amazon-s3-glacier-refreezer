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
from aws_cdk import RemovalPolicy
from aws_cdk.aws_glue_alpha import Job, JobExecutable, PythonVersion, GlueVersion, Code
from cdk_nag import NagSuppressions
from constructs import Construct


class OutputKeys:
    ASYNC_FACILITATOR_TABLE_NAME = "AsyncFacilitatorTableName"
    ASYNC_FACILITATOR_TOPIC_ARN = "AsyncFacilitatorTopicArn"
    OUTPUT_BUCKET_NAME = "OutputBucketName"
    INVENTORY_BUCKET_NAME = "InventoryBucketName"


class RefreezerStack(Stack):
    outputs: dict[str, CfnOutput]

    def __init__(self, scope: Construct, construct_id: str) -> None:
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
                    "reason": (
                        "Output Bucket has server access logs disabled and will be"
                        " addressed later."
                    ),
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

        glue_job = Job(
            self,
            "glueetl",
            executable=JobExecutable.python_etl(
                glue_version=GlueVersion.V1_0,
                python_version=PythonVersion.THREE,
                script=Code.from_bucket(inventory_bucket, "scripts/script.py"),
            ),
            description="an example Python Shell job",
        )

        self.outputs[OutputKeys.INVENTORY_BUCKET_NAME] = CfnOutput(
            self,
            OutputKeys.INVENTORY_BUCKET_NAME,
            value=inventory_bucket.bucket_name,
        )

        nag_suppression_map = {
            inventory_bucket: [
                {
                    "id": "AwsSolutions-S1",
                    "reason": (
                        "Inventory Bucket has server access logs disabled and will be"
                        " addressed later."
                    ),
                }
            ],
            glue_job.role: [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "The role, uses AWS managed policies.	",
                    "appliesTo": [
                        "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSGlueServiceRole",
                    ],
                },
            ],
            glue_job.role.node.try_find_child("DefaultPolicy").node.find_child(  # type: ignore
                "Resource"
            ): [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": (
                        "wildcard applied to specific resource and it should be"
                        " suppressed"
                    ),
                    "appliesTo": [
                        "Action::s3:GetObject*",
                        "Action::s3:GetBucket*",
                        "Action::s3:List*",
                    ],
                },
            ],
            glue_job: [
                {
                    "id": "AwsSolutions-GL1",
                    "reason": (
                        "The Glue job does not use a security configuration with"
                        " CloudWatch Log encryption enabled."
                    ),
                },
                {
                    "id": "AwsSolutions-GL3",
                    "reason": (
                        "The Glue job does not use a security configuration with job"
                        " bookmark encryption enabled."
                    ),
                },
            ],
        }

        for resource, suppressions in nag_suppression_map.items():
            NagSuppressions.add_resource_suppressions(resource, suppressions)  # type: ignore
