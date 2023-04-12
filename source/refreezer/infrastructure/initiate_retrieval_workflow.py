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
from aws_cdk import aws_sns as sns
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from cdk_nag import NagSuppressions
from aws_cdk import CfnElement
from constructs import Construct
from typing import Optional

from refreezer.mocking.mock_glacier_stack import MockingParams
from refreezer.infrastructure.output_keys import OutputKeys
from refreezer.infrastructure.distributed_map import DistributedMap


class InitiateRetrievalWorkflow:
    def __init__(
        self,
        scope: Construct,
        async_facilitator_table: dynamodb.Table,
        glacier_retrieval_table: dynamodb.Table,
        topic: sns.Topic,
        inventory_bucket: s3.Bucket,
        output_bucket: s3.Bucket,
        outputs: dict[str, CfnOutput],
        mock_params: Optional[MockingParams] = None,
    ) -> None:
        # TODO: To be replaced by InitiateJob custom state for Step Function SDK integration
        initiate_retrieval_initiate_job = sfn.Pass(
            scope, "InitiateRetrievalInitiateJob"
        )

        dynamo_db_put_state_json = {
            "Type": "Task",
            "Parameters": {
                "TableName": glacier_retrieval_table.table_name,
                "Item": {
                    "pk": {
                        "S": "IR:$.ArchiveId",
                    },
                    "sk": {
                        "S": "meta",
                    },
                    "job_id": {
                        "S": "$.JobId",
                    },
                    "start_timestamp": {
                        "S": "$$.Execution.StartTime",
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

        initiate_retrieval_inner_distributed_map_state = DistributedMap(
            scope,
            "InitiateRetrievalInnerDistributedMap",
            definition=initiate_retrieval_definition,
            max_concurrency=1,
            item_reader_resource="arn:aws:states:::s3:getObject",
            reader_config={
                "InputType": "CSV",
                "CSVHeaderLocation": "FIRST_ROW",
            },
            item_reader_parameters={"Bucket.$": "$.bucket", "Key.$": "$.item.Key"},
            item_selector={
                "bucket.$": "$.bucket",
                "key.$": "$.item.Key",
                "item.$": "$$.Map.Item.Value",
            },
            result_writer={
                "Resource": "arn:aws:states:::s3:putObject",
                "Parameters": {
                    "Bucket": inventory_bucket.bucket_name,
                    "Prefix.$": "States.Format('{}/initiate_retrieval_inner_distributed_map_output', $.workflow_run)",
                },
            },
            result_path="$.map_result",
        )

        initiate_retrieval_distributed_map_state = DistributedMap(
            scope,
            "InitiateRetrievalDistributedMap",
            definition=initiate_retrieval_inner_distributed_map_state,
            max_concurrency=1,
            item_reader_resource="arn:aws:states:::s3:listObjectsV2",
            item_reader_parameters={
                "Bucket": inventory_bucket.bucket_name,
                "Prefix.$": "States.Format('{}/sorted_inventory', $.workflow_run)",
            },
            item_selector={
                "bucket": inventory_bucket.bucket_name,
                "workflow_run.$": "$.workflow_run",
                "item.$": "$$.Map.Item.Value",
            },
            result_writer={
                "Resource": "arn:aws:states:::s3:putObject",
                "Parameters": {
                    "Bucket": inventory_bucket.bucket_name,
                    "Prefix.$": "States.Format('{}/initiate_retrieval_distributed_map_output', $.workflow_run)",
                },
            },
            result_path="$.map_result",
        )

        initiate_retrieval_state_machine = sfn.StateMachine(
            scope,
            "InitiateRetrievalStateMachine",
            definition=initiate_retrieval_distributed_map_state,
        )

        outputs[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN] = CfnOutput(
            scope,
            OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN,
            value=initiate_retrieval_state_machine.state_machine_arn,
        )

        inventory_bucket.grant_read_write(initiate_retrieval_state_machine)
        glacier_retrieval_table.grant_read_write_data(initiate_retrieval_state_machine)

        initiate_retrieval_state_machine_policy = iam.Policy(
            scope,
            "StateMachinePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[initiate_retrieval_state_machine.state_machine_arn],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:{initiate_retrieval_state_machine.state_machine_name}/*"
                    ],
                ),
            ],
        )

        initiate_retrieval_state_machine_policy.attach_to_role(
            initiate_retrieval_state_machine.role
        )

        assert isinstance(inventory_bucket.node.default_child, CfnElement)
        inventory_bucket_logical_id = Stack.of(scope).get_logical_id(
            inventory_bucket.node.default_child
        )

        NagSuppressions.add_resource_suppressions(
            initiate_retrieval_state_machine.role.node.find_child(
                "DefaultPolicy"
            ).node.find_child("Resource"),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy required for Step Function logging",
                    "appliesTo": ["Resource::*"],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy required to export the results of the Distributed Map state to S3 bucket",
                    "appliesTo": ["Action::s3:Abort*", "Action::s3:DeleteObject*"],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy for reading a file as dataset in a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::<{inventory_bucket_logical_id}.Arn>/*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                },
            ],
        )

        assert isinstance(
            initiate_retrieval_state_machine.node.default_child, CfnElement
        )
        initiate_retrieval_state_machine_logical_id = Stack.of(scope).get_logical_id(
            initiate_retrieval_state_machine.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            initiate_retrieval_state_machine_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy needed to run a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        f"Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:<{initiate_retrieval_state_machine_logical_id}.Name>/*"
                    ],
                }
            ],
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
