"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import (
    Aws,
    Duration,
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
from aws_cdk import aws_stepfunctions_tasks as tasks
from aws_cdk import RemovalPolicy
from cdk_nag import NagSuppressions
from aws_cdk import CfnElement
from constructs import Construct
from typing import Optional, Union

from refreezer.mocking.mock_glacier_stack import MockingParams
from aws_cdk.aws_sns_subscriptions import LambdaSubscription
from aws_cdk.aws_lambda_event_sources import DynamoEventSource


class OutputKeys:
    ASYNC_FACILITATOR_TABLE_NAME = "AFTN"
    ASYNC_FACILITATOR_TOPIC_ARN = "AFTA"
    OUTPUT_BUCKET_NAME = "OBN"
    INVENTORY_BUCKET_NAME = "IBN"
    CHUNK_RETRIEVAL_LAMBDA_ARN = "CRLA"
    CHUNK_VALIDATION_LAMBDA_ARN = "CVLA"
    INVENTORY_CHUNK_RETRIEVAL_LAMBDA_ARN = "ICRLA"
    INVENTORY_RETRIEVAL_STATE_MACHINE_ARN = "IRSMA"
    INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN = "ICDLA"
    ASYNC_FACILITATOR_LAMBDA_NAME = "AFLN"


class RefreezerStack(Stack):
    outputs: dict[str, CfnOutput]

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        mock_params: Optional[MockingParams] = None,
    ) -> None:
        super().__init__(scope, construct_id)

        MAXIMUM_INVENTORY_RECORD_SIZE = 2**10 * 2
        CHUNK_SIZE = 2**20 * 5

        self.outputs = {}

        table = dynamodb.Table(
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
            value=table.table_name,
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

        facilitator_lambda = lambda_.Function(
            self,
            "AsyncFacilitator",
            handler="refreezer.application.handlers.async_facilitator_handler",
            code=lambda_.Code.from_asset("source"),
            runtime=lambda_.Runtime.PYTHON_3_9,
            memory_size=256,
        )

        facilitator_lambda.add_event_source(
            DynamoEventSource(
                table, starting_position=lambda_.StartingPosition.TRIM_HORIZON
            )
        )
        facilitator_lambda.add_environment("DDB_TABLE_NAME", table.table_name)

        topic.add_subscription(LambdaSubscription(facilitator_lambda))

        self.outputs[OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME] = CfnOutput(
            self,
            OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME,
            value=facilitator_lambda.function_name,
        )

        table.grant(facilitator_lambda, *["dynamodb:Query", "dynamodb:PutItem"])
        table.grant_stream(
            facilitator_lambda,
            *[
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams",
            ],
        )

        assert facilitator_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            facilitator_lambda.role,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "CDK grants AWS managed policy for Lambda basic execution by default. Replacing it with a customer managed policy will be addressed later",
                    "appliesTo": [
                        "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
                    ],
                },
            ],
        )

        assert facilitator_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            facilitator_lambda.role.node.find_child("DefaultPolicy").node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "When activating stream for dynamodb table. It automatically allow listStreams to all resources with a wildcard and should be suppressed",
                    "appliesTo": [
                        "Resource::<AsyncFacilitatorTable8420A92A.Arn>/stream/*",
                        "Resource::*",
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Lambda permission needed to SendTaskSuccess and SendTaskFailure.",
                    "appliesTo": [
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:stateMachine:InventoryRetrievalStateMachine*",
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:InventoryRetrievalStateMachine*",
                    ],
                },
            ],
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

        state_json = {
            "Type": "Task",
            "Parameters": {
                "AccountId": Stack.of(self).account,
                "JobParameters": {
                    "Type": "inventory-retrieval",
                    "Description.$": "$.description",
                    "Format": "CSV",
                    "SnsTopic": topic.topic_arn,
                },
                "VaultName.$": "$.vault_name",
            },
            "Resource": "arn:aws:states:::aws-sdk:glacier:initiateJob",
        }

        get_inventory_initiate_job: Union[sfn.IChainable, sfn.INextable]
        get_inventory_initiate_job = sfn.CustomState(
            scope, "GetInventoryInitiateJob", state_json=state_json
        )

        if mock_params is not None:
            get_inventory_initiate_job = mock_params.mock_glacier_initiate_job_task
            mock_notify_sns_lambda_role = iam.Role.from_role_arn(
                self,
                "MockNotifySNSLambdaRole",
                mock_params.mock_notify_sns_lambda_role_arn,
            )
            topic.grant_publish(mock_notify_sns_lambda_role)

        initiate_job_state_policy = iam.Policy(
            self,
            "InitiateJobStatePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glacier:InitiateJob",
                    ],
                    resources=[
                        f"arn:aws:glacier:{Aws.REGION}:{Aws.ACCOUNT_ID}:vaults/*"
                    ],
                ),
            ],
        )

        NagSuppressions.add_resource_suppressions(
            initiate_job_state_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for inventory retrieval initiate job, since the vault name is an input that is not known in advance",
                    "appliesTo": [
                        "Resource::arn:aws:glacier:<AWS::Region>:<AWS::AccountId>:vaults/*"
                    ],
                },
            ],
        )

        # TODO: To be replaced by DynamoDB Put custom state for Step Function SDK integration
        # pause the workflow using waitForTaskToken mechanism
        parameters = {
            "InventorySize": 2**30 * 3,
            "MaximumInventoryRecordSize": MAXIMUM_INVENTORY_RECORD_SIZE,
            "ChunkSize": CHUNK_SIZE,
        }
        dynamo_db_put = sfn.Pass(self, "DynamoDBPut", parameters=parameters)

        inventory_chunk_determination_lambda = lambda_.Function(
            self,
            "InventoryChunkDetermination",
            handler="refreezer.application.handlers.inventory_chunk_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to generate the correct byte offsets to retrieve the inventory.",
        )

        generate_chunk_array_lambda = tasks.LambdaInvoke(
            self,
            "GenerateChunkArrayLambda",
            lambda_function=inventory_chunk_determination_lambda,
            payload_response_only=True,
        )

        self.outputs[OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN] = CfnOutput(
            self,
            OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN,
            value=inventory_chunk_determination_lambda.function_name,
        )

        assert inventory_chunk_determination_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            inventory_chunk_determination_lambda.role.node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "CDK grants AWS managed policy for Lambda basic execution by default. Replacing it with a customer managed policy will be addressed later.",
                    "appliesTo": [
                        "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                },
            ],
        )

        inventory_chunk_download_lambda_function = lambda_.Function(
            self,
            "InventoryChunkDownload",
            handler="refreezer.application.handlers.inventory_chunk_download_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to download inventory chunks from Glacier.",
        )

        assert inventory_chunk_download_lambda_function.role is not None
        NagSuppressions.add_resource_suppressions(
            inventory_chunk_download_lambda_function.role.node.find_child("Resource"),
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

        inventory_chunk_download_lambda = tasks.LambdaInvoke(
            self,
            "InventoryChunkDownloadLambda",
            lambda_function=inventory_chunk_download_lambda_function,
            payload_response_only=True,
        )

        self.outputs[OutputKeys.INVENTORY_CHUNK_RETRIEVAL_LAMBDA_ARN] = CfnOutput(
            self,
            OutputKeys.INVENTORY_CHUNK_RETRIEVAL_LAMBDA_ARN,
            value=inventory_chunk_download_lambda_function.function_name,
        )

        # TODO: To be replaced by Map state in Distributed mode
        distributed_map_state = sfn.Map(self, "DistributedMap", items_path="$.body")
        distributed_map_state.iterator(inventory_chunk_download_lambda)

        # TODO: To be replaced by Glue task
        glue_order_archives = sfn.Pass(self, "GlueOrderArchives")

        # TODO: To be replaced by InventoryValidationLambda LambdaInvoke task
        inventory_validation_lambda = sfn.Pass(self, "InventoryValidationLambda")

        glue_order_archives.next(inventory_validation_lambda)

        get_inventory_initiate_job.next(dynamo_db_put).next(
            generate_chunk_array_lambda
        ).next(distributed_map_state).next(glue_order_archives)

        definition = (
            sfn.Choice(self, "Provided Inventory?")
            .when(
                sfn.Condition.string_equals("$.provided_inventory", "YES"),
                glue_order_archives,
            )
            .otherwise(get_inventory_initiate_job)
        )

        inventory_retrieval_state_machine = sfn.StateMachine(
            self, "InventoryRetrievalStateMachine", definition=definition
        )

        facilitator_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "states:SendTaskSuccess",
                    "states:DescribeExecution",
                    "states:SendTaskFailure",
                ],
                resources=[
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:InventoryRetrievalStateMachine*",
                    f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:InventoryRetrievalStateMachine*",
                ],
            ),
        )

        initiate_job_state_policy.attach_to_role(inventory_retrieval_state_machine.role)

        self.outputs[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN] = CfnOutput(
            self,
            OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN,
            value=inventory_retrieval_state_machine.state_machine_arn,
        )

        assert isinstance(
            inventory_chunk_download_lambda_function.node.default_child, CfnElement
        )
        assert inventory_chunk_download_lambda_function.role is not None
        inventory_chunk_download_lambda_function_logical_id = Stack.of(
            self
        ).get_logical_id(inventory_chunk_download_lambda_function.node.default_child)

        NagSuppressions.add_resource_suppressions(
            inventory_retrieval_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "By default wildcard permission is granted to the lambda.  This will be replaced with a proper IAM role later.",
                    "appliesTo": [
                        "Resource::<"
                        + inventory_chunk_download_lambda_function_logical_id
                        + ".Arn>:*"
                    ],
                }
            ],
        )

        assert isinstance(
            inventory_chunk_determination_lambda.node.default_child, CfnElement
        )
        assert inventory_chunk_determination_lambda.role is not None
        inventory_chunk_determination_lambda_logical_id = Stack.of(self).get_logical_id(
            inventory_chunk_determination_lambda.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            inventory_retrieval_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "By default wildcard permission is granted to the lambda.  This will be replaced with a proper IAM role later.",
                    "appliesTo": [
                        "Resource::<"
                        + inventory_chunk_determination_lambda_logical_id
                        + ".Arn>:*"
                    ],
                }
            ],
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

        boto3_lambda_layer = lambda_.LayerVersion(
            self,
            "boto3_lambda_layer",
            code=lambda_.Code.from_asset("layers/boto3.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
            license="Apache-2.0",
            description="A Boto3 layer to use (v1.26.70) rather than the default provided by lambda",
        )

        chunk_retrieval_lambda = lambda_.Function(
            self,
            "ChunkRetrieval",
            handler="refreezer.application.handlers.chunk_retrieval_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            layers=[boto3_lambda_layer],
            memory_size=2560,
            timeout=Duration.minutes(15),
            description="Lambda to retrieve chunks from Glacier, upload them to S3 and generate file checksums.",
        )

        get_job_output_policy = iam.Policy(
            self,
            "GetJobOutputPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glacier:GetJobOutput",
                        "s3:PutObject",
                    ],
                    resources=[
                        f"arn:aws:glacier:{Aws.REGION}:{Aws.ACCOUNT_ID}:vaults/*",
                        f"arn:aws:s3:::{output_bucket.bucket_name}/*",
                    ],
                ),
            ],
        )

        NagSuppressions.add_resource_suppressions(
            get_job_output_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for inventory retrieval initiate job, since the vault name is an input that is not known in advance",
                    "appliesTo": [
                        "Resource::arn:aws:glacier:<AWS::Region>:<AWS::AccountId>:vaults/*",
                    ],
                },
            ],
        )

        NagSuppressions.add_resource_suppressions(
            get_job_output_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "It's necessary to have wildcard permissions for s3 put object, to allow for copying glacier archives over to s3 in any location",
                },
            ],
        )

        assert chunk_retrieval_lambda.role is not None
        get_job_output_policy.attach_to_role(chunk_retrieval_lambda.role)

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

        chunk_validation_lambda = lambda_.Function(
            self,
            "ChunkValidation",
            handler="refreezer.application.handlers.chunk_validation_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            layers=[boto3_lambda_layer],
            memory_size=128,
            timeout=Duration.minutes(3),
            description="Lambda to validate retrieved chunks and complete the S3 multipart upload.",
        )

        self.outputs[OutputKeys.CHUNK_VALIDATION_LAMBDA_ARN] = CfnOutput(
            self,
            OutputKeys.CHUNK_VALIDATION_LAMBDA_ARN,
            value=chunk_validation_lambda.function_name,
        )

        assert chunk_validation_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            chunk_validation_lambda.role.node.find_child("Resource"),
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
