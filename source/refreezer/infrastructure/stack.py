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
from aws_cdk.aws_glue_alpha import Job, JobExecutable, PythonVersion, GlueVersion, Code

from refreezer.infrastructure.distributed_map import DistributedMap
from refreezer.infrastructure.nested_distributed_map import NestedDistributedMap


from refreezer.infrastructure.glue_helper.glue_sfn_update import GlueSfnUpdate


class OutputKeys:
    ARCHIVE_CHUNK_DETERMINATION_LAMBDA_ARN = "ArchiveChunkDeterminationLambdaArn"
    ASYNC_FACILITATOR_TABLE_NAME = "AsyncFacilitatorTableName"
    ASYNC_FACILITATOR_TOPIC_ARN = "AsyncFacilitatorTopicArn"
    OUTPUT_BUCKET_NAME = "OutputBucketName"
    INVENTORY_BUCKET_NAME = "InventoryBucketName"
    CHUNK_RETRIEVAL_LAMBDA_ARN = "ChunkRetrievalLambdaArn"
    CHUNK_VALIDATION_LAMBDA_ARN = "ChunkValidationLambdaArn"
    INVENTORY_CHUNK_RETRIEVAL_LAMBDA_ARN = "InventoryChunkRetrievalLambdaArn"
    INVENTORY_RETRIEVAL_STATE_MACHINE_ARN = "InventoryRetrievalStateMachineArn"
    INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN = "InventoryChunkDeterminationLambdaArn"
    ASYNC_FACILITATOR_LAMBDA_NAME = "AsyncFacilitatorLambdaName"
    INITIATE_RETRIEVAL_STATE_MACHINE_ARN = "InitiateRetrievalStateMachineArn"
    RETRIEVE_ARCHIVE_STATE_MACHINE_ARN = "RetrieveArchiveStateMachineArn"
    GLACIER_RETRIEVAL_TABLE_NAME = "GlacierRetrievalTableName"
    INVENTORY_VALIDATE_MULTIPART_UPLOAD_LAMBDA_ARN = (
        "InventoryValidateMultipartUploadLambdaArn"
    )


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
        GLUE_MAX_CONCURENT_RUNS = 10

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

        assert isinstance(table.node.default_child, CfnElement)
        async_facilitator_table_logical_id = Stack.of(self).get_logical_id(
            table.node.default_child
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
                        f"Resource::<{async_facilitator_table_logical_id}.Arn>/stream/*",
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

        # =============================================================================
        # ==========================  Get Inventory Workflow  =========================
        # =============================================================================

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
            "ResultPath": "$.initiate_job_result",
            "Resource": "arn:aws:states:::aws-sdk:glacier:initiateJob",
        }
        get_inventory_initiate_job: Union[sfn.IChainable, sfn.INextable]
        get_inventory_initiate_job = sfn.CustomState(
            scope, "GetInventoryInitiateJob", state_json=state_json
        )

        if mock_params is not None:
            get_inventory_initiate_job = (
                mock_params.mock_glacier_inventory_initiate_job_task
            )
            mock_notify_sns_lambda_role = iam.Role.from_role_arn(
                self,
                "MockNotifySNSLambdaRole",
                mock_params.mock_notify_sns_lambda_role_arn,
            )
            topic.grant_publish(mock_notify_sns_lambda_role)

        glue_job_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            inline_policies={
                "GlueS3Policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["s3:PutObject", "s3:GetObject"],
                            resources=[f"{inventory_bucket.bucket_arn}/*"],
                        )
                    ]
                )
            },
        )
        glue_job_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )

        # TODO soon should be updated for now it is hardcoded
        workflow_run_id = "workflow_run_id"
        glue_script_location = f"{workflow_run_id}/scripts/inventory_sort_script.py"
        glue_job = Job(
            self,
            "GlueOrderingJob",
            executable=JobExecutable.python_etl(
                glue_version=GlueVersion.V3_0,
                python_version=PythonVersion.THREE,
                script=Code.from_bucket(inventory_bucket, glue_script_location),
            ),
            role=glue_job_role,
        )

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
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["glue:UpdateJob", "glue:StartJobRun"],
                    resources=[
                        f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:job/{glue_job.job_name}"
                    ],
                ),
            ],
        )

        NagSuppressions.add_resource_suppressions(
            glue_job,
            [
                {
                    "id": "AwsSolutions-GL1",
                    "reason": "The Glue job does not use a security configuration with CloudWatch Log encryption enabled. Will be addressed later",
                },
                {
                    "id": "AwsSolutions-GL3",
                    "reason": "The Glue job does not use a security configuration with CloudWatch Log encryption enabled. Will be addressed later",
                },
            ],
        )

        assert isinstance(inventory_bucket.node.default_child, CfnElement)
        inventory_bucket_logical_id = Stack.of(self).get_logical_id(
            inventory_bucket.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            glue_job_role,
            [
                {
                    "id": "AwsSolutions-IAM4",
                    "reason": "By default Put object is not provided by the glue job default role. ",
                    "appliesTo": [
                        "Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSGlueServiceRole"
                    ],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Making sure that we can put various objects in the bucket. Output and the script should be store in this bucket ",
                    "appliesTo": [f"Resource::<{inventory_bucket_logical_id}.Arn>/*"],
                },
            ],
        )

        NagSuppressions.add_resource_suppressions(
            glue_job_role.node.find_child("DefaultPolicy").node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Automatically the gluejob default added these wildcarded values. Hence the wildcard needs to suppressed.",
                    "appliesTo": [
                        "Action::s3:GetObject*",
                        "Action::s3:GetBucket*",
                        "Action::s3:List*",
                    ],
                },
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

        dynamo_db_put_state_json = {
            "Type": "Task",
            "Parameters": {
                "TableName": table.table_name,
                "Item": {
                    "task_token": {
                        "S.$": "$$.Task.Token",
                    },
                    "job_id": {
                        "S.$": "$.initiate_job_result.JobId",
                    },
                    "start_timestamp": {
                        "S.$": "$$.Execution.StartTime",
                    },
                },
            },
            "ResultPath": "$.async_ddb_put_result",
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem.waitForTaskToken",
        }

        dynamo_db_put = sfn.CustomState(
            scope, "AsyncFacilitatorDynamoDBPut", state_json=dynamo_db_put_state_json
        )

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
            payload=sfn.TaskInput.from_object(
                {
                    "InventorySize": sfn.JsonPath.string_at(
                        "$.async_ddb_put_result.job_result.InventorySizeInBytes"
                    ),
                    "MaximumInventoryRecordSize": MAXIMUM_INVENTORY_RECORD_SIZE,
                    "ChunkSize": CHUNK_SIZE,
                }
            ),
            result_path="$.chunking_result",
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

        initiate_S3_multipart_upload = tasks.CallAwsService(
            self,
            "InitiateInventoryMultipartUpload",
            service="S3",
            action="createMultipartUpload",
            iam_resources=[inventory_bucket.bucket_arn],
            parameters={
                "Bucket": inventory_bucket.bucket_name,
                "ContentType": "text/csv",
                "Key": "inventory.csv",
            },
            result_path="$.multipart_upload_result",
        )

        dynamo_db_put_upload_id = tasks.CallAwsService(
            self,
            "PutInventoryMultipartUploadMetadata",
            service="DynamoDB",
            action="putItem",
            iam_resources=[glacier_retrieval_table.table_arn],
            parameters={
                "TableName": glacier_retrieval_table.table_name,
                "Item": {
                    "pk": {
                        "S.$": "States.Format('{}:{}', $.workflow_run, $.vault_name)"
                    },
                    "sk": {"S": "meta"},
                    "job_id": {
                        "S.$": "$.initiate_job_result.JobId",
                    },
                    "execution_start_time": {
                        "S.$": "$$.Execution.StartTime",
                    },
                    "description": {
                        "S.$": "$.description",
                    },
                    "vault_name": {
                        "S.$": "$.vault_name",
                    },
                    "inventory_size": {
                        "S.$": "States.JsonToString($.async_ddb_put_result.job_result.InventorySizeInBytes)",
                    },
                    "upload_id": {
                        "S.$": "$.multipart_upload_result.UploadId",
                    },
                },
            },
            result_path="$.multipart_upload_result.UploadId",
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

        distributed_map_state = DistributedMap(
            self,
            "InventoryChunkRetrievalDistributedMap",
            definition=inventory_chunk_download_lambda,
            items_path="$.chunking_result.body",
            item_selector={
                "JobId.$": "$.initiate_job_result.JobId",
                "VaultName.$": "$.vault_name",
                "ByteRange.$": "$$.Map.Item.Value",
                "S3DestinationBucket": inventory_bucket.bucket_name,
                "S3DestinationKey.$": "States.Format('{}/inventory.csv', $.workflow_run)",
                "UploadId.$": "$.multipart_upload_result.UploadId",
                "PartNumber.$": "$$.Map.Item.Index",
            },
        )

        glue_sfn_update = GlueSfnUpdate(
            self,
            "GlueSfnUpdate",
            inventory_bucket.bucket_name,
            inventory_bucket.bucket_arn,
            glue_job.job_name,
            glue_job.role.role_arn,
            glue_script_location,
            glue_max_concurent_runs=GLUE_MAX_CONCURENT_RUNS
            if mock_params is not None
            else 1,
        )

        glue_order_archives = glue_sfn_update.autogenerate_etl_script().next(
            glue_sfn_update.start_job()
        )

        validate_multipart_lambda = lambda_.Function(
            self,
            "InventoryValidateMultipartUpload",
            handler="refreezer.application.handlers.validate_multipart_inventory_upload",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to close the multipart inventory upload.",
        )

        self.outputs[
            OutputKeys.INVENTORY_VALIDATE_MULTIPART_UPLOAD_LAMBDA_ARN
        ] = CfnOutput(
            self,
            OutputKeys.INVENTORY_VALIDATE_MULTIPART_UPLOAD_LAMBDA_ARN,
            value=validate_multipart_lambda.function_name,
        )

        validate_multipart_task = tasks.LambdaInvoke(
            self,
            "ValidateMultipartUploadLambdaTask",
            lambda_function=validate_multipart_lambda,
            payload_response_only=True,
        )

        assert validate_multipart_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            validate_multipart_lambda.role.node.find_child("Resource"),
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

        assert isinstance(validate_multipart_lambda.node.default_child, CfnElement)
        assert validate_multipart_lambda.role is not None
        validate_multipart_lambda_logical_id = Stack.of(self).get_logical_id(
            validate_multipart_lambda.node.default_child
        )

        get_inventory_initiate_job.next(dynamo_db_put).next(
            generate_chunk_array_lambda
        ).next(initiate_S3_multipart_upload).next(dynamo_db_put_upload_id).next(
            distributed_map_state
        ).next(
            validate_multipart_task
        ).next(
            glue_order_archives
        )

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

        inventory_chunk_download_lambda_function.grant_invoke(
            inventory_retrieval_state_machine
        )

        inventory_bucket.grant_put(inventory_retrieval_state_machine)
        initiate_job_state_policy.attach_to_role(inventory_retrieval_state_machine.role)
        table.grant_read_write_data(inventory_retrieval_state_machine)

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
                    "reason": "By default wildcard permission is granted to the state machine from various sources.  This will be replaced with a proper IAM role later.",
                    "appliesTo": [
                        f"Resource::<{inventory_chunk_download_lambda_function_logical_id}.Arn>:*"
                    ],
                }
            ],
        )

        inventory_retrieval_state_machine_policy = iam.Policy(
            self,
            "InventoryRetrievalStateMachinePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[inventory_retrieval_state_machine.state_machine_arn],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{inventory_retrieval_state_machine.state_machine_name}/*"
                    ],
                ),
            ],
        )

        inventory_retrieval_state_machine_policy.attach_to_role(
            inventory_retrieval_state_machine.role
        )

        assert isinstance(
            inventory_retrieval_state_machine.node.default_child, CfnElement
        )
        inventory_retrieval_state_machine_logical_id = Stack.of(self).get_logical_id(
            inventory_retrieval_state_machine.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            inventory_retrieval_state_machine_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy needed to run a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:<{inventory_retrieval_state_machine_logical_id}.Name>/*"
                    ],
                }
            ],
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
                        f"Resource::<{validate_multipart_lambda_logical_id}.Arn>:*"
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
                        f"Resource::<{inventory_chunk_determination_lambda_logical_id}.Arn>:*",
                        "Action::s3:Abort*",
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
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

        archive_chunk_determination_lambda = lambda_.Function(
            self,
            "ArchiveChunkDetermination",
            handler="refreezer.application.handlers.archive_chunk_determination_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to generate the correct byte offsets to retrieve the archive.",
        )

        # TODO: Add task to archive state machine once PR 100 is merged
        # generate_archive_chunk_array_lambda = tasks.LambdaInvoke(
        #     self,
        #     "GenerateArchiveChunkArrayLambda",
        #     lambda_function=archive_chunk_determination_lambda,
        #     payload_response_only=True,
        # )

        self.outputs[OutputKeys.ARCHIVE_CHUNK_DETERMINATION_LAMBDA_ARN] = CfnOutput(
            self,
            OutputKeys.ARCHIVE_CHUNK_DETERMINATION_LAMBDA_ARN,
            value=archive_chunk_determination_lambda.function_name,
        )

        assert archive_chunk_determination_lambda.role is not None
        NagSuppressions.add_resource_suppressions(
            archive_chunk_determination_lambda.role.node.find_child("Resource"),
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
            memory_size=1536,
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

        # =============================================================================
        # ======================  Initiate Retrieval Workflow  ========================
        # =============================================================================

        state_json = {
            "Type": "Task",
            "Parameters": {
                "AccountId": Aws.ACCOUNT_ID,
                "JobParameters": {
                    "Type": "archive-retrieval",
                    "ArchiveId.$": "$.archive_id",
                    "Description.$": "$.description",
                    "SnsTopic": topic.topic_arn,
                    "Tier.$": "$.tier",
                },
                "VaultName.$": "$.vault_name",
                "ResultPath": "$.initiate_job_result",
            },
            "Resource": "arn:aws:states:::aws-sdk:glacier:initiateJob",
        }

        initiate_retrieval_initiate_job: Union[sfn.IChainable, sfn.INextable]
        initiate_retrieval_initiate_job = sfn.CustomState(
            scope, "InitiateRetrievalInitiateJob", state_json=state_json
        )

        if mock_params is not None:
            initiate_retrieval_initiate_job = (
                mock_params.mock_glacier_archive_initiate_job_task
            )

        dynamo_db_put_state_json = {
            "Type": "Task",
            "Parameters": {
                "TableName": glacier_retrieval_table.table_name,
                "Item": {
                    "pk": {
                        "S.$": "States.Format('IR:{}', $.ArchiveId)",
                    },
                    "sk": {
                        "S": "meta",
                    },
                    "job_id": {
                        "S.$": "$.JobId",
                    },
                    "start_timestamp": {
                        "S.$": "$$.Execution.StartTime",
                    },
                },
            },
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem",
        }

        initiate_retrieval_dynamo_db_put = sfn.CustomState(
            scope,
            "InitiateRetrievalWorkflowDynamoDBPut",
            state_json=dynamo_db_put_state_json,
        )

        initiate_retrieval_definition = initiate_retrieval_initiate_job.next(
            initiate_retrieval_dynamo_db_put
        )

        initiate_retrieval_distributed_map = NestedDistributedMap(
            self, "InitiateRetrieval", initiate_retrieval_definition, inventory_bucket
        )

        initiate_retrieval_state_machine = sfn.StateMachine(
            self,
            "InitiateRetrievalStateMachine",
            definition=initiate_retrieval_distributed_map.distributed_map_state,
        )

        initiate_job_state_policy.attach_to_role(initiate_retrieval_state_machine.role)

        self.outputs[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN] = CfnOutput(
            self,
            OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN,
            value=initiate_retrieval_state_machine.state_machine_arn,
        )

        glacier_retrieval_table.grant_read_write_data(initiate_retrieval_state_machine)

        initiate_retrieval_distributed_map.configure_step_function(
            self,
            "InitiateRetrieval",
            initiate_retrieval_state_machine,
            inventory_bucket,
        )

        NagSuppressions.add_resource_suppressions(
            initiate_retrieval_state_machine,
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

        # =============================================================================
        # =======================  Retrieve Archive Workflow  =========================
        # =============================================================================

        # TODO: To be replaced by DynamoDB GetItem (Synchronous mode)
        retrieve_archive_dynamo_db_get_job = sfn.Pass(
            self, "RetrieveArchiveDynamoDBGetJob"
        )

        # TODO: To be replaced by DynamoDB Put custom state for Step Function SDK integration
        # pause the workflow using waitForTaskToken mechanism
        retrieve_archive_dynamo_db_put = sfn.Pass(self, "RetrieveArchiveDynamoDBPut")

        # TODO: To be replaced by s3:createMultipartUpload task
        retrieve_archive_start_multipart_upload = sfn.Pass(
            self, "RetrieveArchiveStartMultipartUpload"
        )

        # TODO: To be replaced by generate chunk array LambdaInvoke task
        retrieve_archive_generate_chunk_array_lambda_task = sfn.Pass(
            self,
            "RetrieveArchiveGenerateChunkArrayLambda",
            parameters={"chunk_array": ["0-499", "500-930"]},
        )

        # TODO: To be replaced by chunk processing LambdaInvoke task
        retrieve_archive_chunk_processing_lambda_task = sfn.Pass(
            self, "RetrieveArchivechunkProcessingLambdaTask"
        )

        # TODO: To be replaced by a Map state in Distributed mode
        retrieve_archive_chunk_distributed_map_state = sfn.Map(
            self, "RetrieveArchiveChunkDistributedMap", items_path="$.chunk_array"
        )
        retrieve_archive_chunk_distributed_map_state.iterator(
            retrieve_archive_chunk_processing_lambda_task
        )

        retrieve_archive_definition = (
            retrieve_archive_dynamo_db_get_job.next(retrieve_archive_dynamo_db_put)
            .next(retrieve_archive_start_multipart_upload)
            .next(retrieve_archive_generate_chunk_array_lambda_task)
            .next(retrieve_archive_chunk_distributed_map_state)
        )

        retrieve_archive_distributed_map = NestedDistributedMap(
            self, "RetrieveArchive", retrieve_archive_definition, inventory_bucket
        )

        # TODO: To be replaced by validate LambdaInvoke task
        retrieve_archive_validate_lambda_task = sfn.Pass(
            self, "RetrieveArchiveValidateLambdaTask"
        )

        # TODO: To be replaced by s3:abortMultipartUpload
        retrieve_archive_close_multipart_upload = sfn.Pass(
            self, "RetrieveArchiveCloseMultipartUpload"
        )

        retrieve_archive_distributed_map_state = (
            retrieve_archive_distributed_map.distributed_map_state
        )
        retrieve_archive_state_machine = sfn.StateMachine(
            self,
            "RetrieveArchiveStateMachine",
            definition=retrieve_archive_distributed_map_state.next(
                retrieve_archive_validate_lambda_task.next(
                    retrieve_archive_close_multipart_upload
                )
            ),
        )

        self.outputs[OutputKeys.RETRIEVE_ARCHIVE_STATE_MACHINE_ARN] = CfnOutput(
            self,
            OutputKeys.RETRIEVE_ARCHIVE_STATE_MACHINE_ARN,
            value=retrieve_archive_state_machine.state_machine_arn,
        )

        retrieve_archive_distributed_map.configure_step_function(
            self, "RetrieveArchive", retrieve_archive_state_machine, inventory_bucket
        )

        NagSuppressions.add_resource_suppressions(
            retrieve_archive_state_machine,
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
