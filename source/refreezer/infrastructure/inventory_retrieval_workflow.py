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
from cdk_nag import NagSuppressions
from aws_cdk import CfnElement
from constructs import Construct
from typing import Optional, Union

from refreezer.mocking.mock_glacier_stack import MockingParams
from aws_cdk.aws_glue_alpha import Job, JobExecutable, PythonVersion, GlueVersion, Code
from refreezer.infrastructure.distributed_map import DistributedMap
from refreezer.infrastructure.output_keys import OutputKeys
from refreezer.infrastructure.glue_helper.glue_sfn_update import GlueSfnUpdate


class InventoryRetrievalWorkflow:
    def __init__(
        self,
        scope: Construct,
        async_facilitator_table: dynamodb.Table,
        topic: sns.Topic,
        inventory_bucket: s3.Bucket,
        output_bucket: s3.Bucket,
        outputs: dict[str, CfnOutput],
        mock_params: Optional[MockingParams] = None,
    ) -> None:
        MAXIMUM_INVENTORY_RECORD_SIZE = 2**10 * 2
        CHUNK_SIZE = 2**20 * 5
        GLUE_MAX_CONCURENT_RUNS = 10

        # TODO soon should be updated for now it is hardcoded
        workflow_run_id = "workflow_run_id"

        state_json = {
            "Type": "Task",
            "Parameters": {
                "AccountId": Stack.of(scope).account,
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
                scope,
                "MockNotifySNSLambdaRole",
                mock_params.mock_notify_sns_lambda_role_arn,
            )
            topic.grant_publish(mock_notify_sns_lambda_role)

        glue_job_role = iam.Role(
            scope,
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

        glue_script_location = f"{workflow_run_id}/scripts/inventory_sort_script.py"
        glue_job = Job(
            scope,
            "GlueOrderingJob",
            executable=JobExecutable.python_etl(
                glue_version=GlueVersion.V3_0,
                python_version=PythonVersion.THREE,
                script=Code.from_bucket(inventory_bucket, glue_script_location),
            ),
            role=glue_job_role,
        )

        initiate_job_state_policy = iam.Policy(
            scope,
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
        inventory_bucket_logical_id = Stack.of(scope).get_logical_id(
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
                "TableName": async_facilitator_table.table_name,
                "Item": {
                    "task_token": {
                        "S.$": "$$.Task.Token",
                    },
                    "job_id": {
                        "S.$": "$.JobId",
                    },
                    "start_timestamp": {
                        "S.$": "$$.Execution.StartTime",
                    },
                },
            },
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:putItem.waitForTaskToken",
        }

        dynamo_db_put = sfn.CustomState(
            scope, "AsyncFacilitatorDynamoDBPut", state_json=dynamo_db_put_state_json
        )

        inventory_chunk_determination_lambda = lambda_.Function(
            scope,
            "InventoryChunkDetermination",
            handler="refreezer.application.handlers.inventory_chunk_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to generate the correct byte offsets to retrieve the inventory.",
        )

        generate_chunk_array_lambda = tasks.LambdaInvoke(
            scope,
            "GenerateChunkArrayLambda",
            lambda_function=inventory_chunk_determination_lambda,
            payload_response_only=True,
            payload=sfn.TaskInput.from_object(
                {
                    "InventorySize": sfn.JsonPath.string_at(
                        "$.job_result.InventorySizeInBytes"
                    ),
                    "MaximumInventoryRecordSize": MAXIMUM_INVENTORY_RECORD_SIZE,
                    "ChunkSize": CHUNK_SIZE,
                }
            ),
        )

        outputs[OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN] = CfnOutput(
            scope,
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
            scope,
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
            scope,
            "InventoryChunkDownloadLambda",
            lambda_function=inventory_chunk_download_lambda_function,
            payload_response_only=True,
        )

        outputs[OutputKeys.INVENTORY_CHUNK_RETRIEVAL_LAMBDA_ARN] = CfnOutput(
            scope,
            OutputKeys.INVENTORY_CHUNK_RETRIEVAL_LAMBDA_ARN,
            value=inventory_chunk_download_lambda_function.function_name,
        )

        distributed_map_state = DistributedMap(
            scope,
            "InventoryChunkRetrievalDistributedMap",
            definition=inventory_chunk_download_lambda,
            items_path="$.body",
        )

        glue_sfn_update = GlueSfnUpdate(
            scope,
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
            scope,
            "InventoryValidateMultipartUpload",
            handler="refreezer.application.handlers.validate_multipart_inventory_upload",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to close the multipart inventory upload.",
        )

        outputs[OutputKeys.INVENTORY_VALIDATE_MULTIPART_UPLOAD_LAMBDA_ARN] = CfnOutput(
            scope,
            OutputKeys.INVENTORY_VALIDATE_MULTIPART_UPLOAD_LAMBDA_ARN,
            value=validate_multipart_lambda.function_name,
        )

        validate_multipart_task = tasks.LambdaInvoke(
            scope,
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
        validate_multipart_lambda_logical_id = Stack.of(scope).get_logical_id(
            validate_multipart_lambda.node.default_child
        )

        get_inventory_initiate_job.next(dynamo_db_put).next(
            generate_chunk_array_lambda
        ).next(distributed_map_state).next(validate_multipart_task).next(
            glue_order_archives
        )

        definition = (
            sfn.Choice(scope, "Provided Inventory?")
            .when(
                sfn.Condition.string_equals("$.provided_inventory", "YES"),
                glue_order_archives,
            )
            .otherwise(get_inventory_initiate_job)
        )

        inventory_retrieval_state_machine = sfn.StateMachine(
            scope, "InventoryRetrievalStateMachine", definition=definition
        )

        inventory_chunk_download_lambda_function.grant_invoke(
            inventory_retrieval_state_machine
        )

        inventory_bucket.grant_put(inventory_retrieval_state_machine)
        initiate_job_state_policy.attach_to_role(inventory_retrieval_state_machine.role)
        async_facilitator_table.grant_read_write_data(inventory_retrieval_state_machine)

        outputs[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN] = CfnOutput(
            scope,
            OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN,
            value=inventory_retrieval_state_machine.state_machine_arn,
        )

        assert isinstance(
            inventory_chunk_download_lambda_function.node.default_child, CfnElement
        )
        assert inventory_chunk_download_lambda_function.role is not None
        inventory_chunk_download_lambda_function_logical_id = Stack.of(
            scope
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
            scope,
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
        inventory_retrieval_state_machine_logical_id = Stack.of(scope).get_logical_id(
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
        inventory_chunk_determination_lambda_logical_id = Stack.of(
            scope
        ).get_logical_id(inventory_chunk_determination_lambda.node.default_child)

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
            scope,
            "ArchiveChunkDetermination",
            handler="refreezer.application.handlers.archive_chunk_determination_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            description="Lambda to generate the correct byte offsets to retrieve the archive.",
        )

        # TODO: Add task to archive state machine once PR 100 is merged
        # generate_archive_chunk_array_lambda = tasks.LambdaInvoke(
        #     scope,
        #     "GenerateArchiveChunkArrayLambda",
        #     lambda_function=archive_chunk_determination_lambda,
        #     payload_response_only=True,
        # )

        outputs[OutputKeys.ARCHIVE_CHUNK_DETERMINATION_LAMBDA_ARN] = CfnOutput(
            scope,
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
            scope,
            "boto3_lambda_layer",
            code=lambda_.Code.from_asset("layers/boto3.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
            license="Apache-2.0",
            description="A Boto3 layer to use (v1.26.70) rather than the default provided by lambda",
        )

        chunk_retrieval_lambda = lambda_.Function(
            scope,
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
            scope,
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

        outputs[OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN] = CfnOutput(
            scope,
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
            scope,
            "ChunkValidation",
            handler="refreezer.application.handlers.chunk_validation_lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("source"),
            layers=[boto3_lambda_layer],
            memory_size=128,
            timeout=Duration.minutes(3),
            description="Lambda to validate retrieved chunks and complete the S3 multipart upload.",
        )

        outputs[OutputKeys.CHUNK_VALIDATION_LAMBDA_ARN] = CfnOutput(
            scope,
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
