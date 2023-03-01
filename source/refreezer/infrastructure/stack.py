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
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import RemovalPolicy
from cdk_nag import NagSuppressions
from constructs import Construct


class OutputKeys:
    ASYNC_FACILITATOR_TABLE_NAME = "AsyncFacilitatorTableName"
    ASYNC_FACILITATOR_TOPIC_ARN = "AsyncFacilitatorTopicArn"
    OUTPUT_BUCKET_NAME = "OutputBucketName"
    INVENTORY_BUCKET_NAME = "InventoryBucketName"
    CHUNK_RETRIEVAL_LAMBDA_ARN = "ChunkRetrievalLambdaArn"
    INVENTORY_RETRIEVAL_STATE_MACHINE_ARN = "InventoryRetrievalStateMachineArn"


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

        # TODO: To be replaced by InitiateJob custom state for Step Function SDK integration
        initiate_job = sfn.Pass(self, "InitiateJob")

        # TODO: To be replaced by DynamoDB Put custom state for Step Function SDK integration
        # pause the workflow using waitForTaskToken mechanism
        dynamo_db_put = sfn.Pass(self, "DynamoDBPut")

        # TODO: To be replaced by GenerateChunkArray LambdaInvoke task
        # which will retrun the chunks array
        parameters = {"chunk_array": ["0-499", "300-799"]}
        generate_chunk_array_lambda = sfn.Pass(
            self, "GenerateChunkArrayLambda", parameters=parameters
        )

        # TODO: To be replaced by InventoryChunkDownload LambdaInvoke task
        inventory_chunk_download_lambda = sfn.Pass(
            self,
            "InventoryChunkDownloadLambda",
            parameters={"InventoryRetrieved": "TRUE"},
        )

        # TODO: To be replaced by Map state in Distributed mode
        distributed_map_state = sfn.Map(
            self, "DistributedMap", items_path="$.chunk_array"
        )
        distributed_map_state.iterator(inventory_chunk_download_lambda)

        # TODO: To be replaced by Glue task
        glue_order_archives = sfn.Pass(self, "GlueOrderArchives")

        # TODO: To be replaced by InventoryValidationLambda LambdaInvoke task
        inventory_validation_lambda = sfn.Pass(self, "InventoryValidationLambda")

        glue_order_archives.next(inventory_validation_lambda)

        initiate_job.next(dynamo_db_put).next(generate_chunk_array_lambda).next(
            distributed_map_state
        ).next(glue_order_archives)

        definition = (
            sfn.Choice(self, "Provided Inventory?")
            .when(
                sfn.Condition.string_equals("$.provided_inventory", "YES"),
                glue_order_archives,
            )
            .otherwise(initiate_job)
        )

        inventory_retrieval_state_machine = sfn.StateMachine(
            self, "InventoryRetrievalStateMachine", definition=definition
        )

        self.outputs[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN] = CfnOutput(
            self,
            OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN,
            value=inventory_retrieval_state_machine.state_machine_arn,
        )

        NagSuppressions.add_resource_suppressions(
            inventory_retrieval_state_machine,
            [
                {
                    "id": "AwsSolutions-SF1",
                    "reason": "Step Function logging is disabled and will be addressed later.",
                },
                {
                    "id": "AwsSolutions-SF2",
                    "reason": "Step Function X-Ray tracing is disabled and will be addressed later.",
                },
            ],
        )

        chunk_retrieval_lambda = lambda_.Function(
            self,
            "ChunkRetrieval",
            handler="refreezer.application.handlers.chunk_retrieval_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to retrieve chunks from Glacier, upload them to S3 and generate file checksums.",
        )

        self.outputs[OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN] = CfnOutput(
            self,
            OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN,
            value=chunk_retrieval_lambda.function_name,
        )

        assert chunk_retrieval_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            chunk_retrieval_lambda.role.node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "CDK grants AWS managed policy for Lambda basic execution by default. Replacing it with a customer managed policy will be addressed later.",
                    "appliesTo": [
                        "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                }
            ],
        )
