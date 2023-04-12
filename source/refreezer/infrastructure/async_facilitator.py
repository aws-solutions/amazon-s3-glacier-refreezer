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
from aws_cdk import aws_lambda as lambda_
from cdk_nag import NagSuppressions
from constructs import Construct
from aws_cdk import CfnElement

from aws_cdk.aws_sns_subscriptions import LambdaSubscription
from aws_cdk.aws_lambda_event_sources import DynamoEventSource
from refreezer.infrastructure.output_keys import OutputKeys


class AsyncFacilitator:
    def __init__(
        self,
        scope: Construct,
        async_facilitator_table: dynamodb.Table,
        topic: sns.Topic,
        outputs: dict[str, CfnOutput],
    ) -> None:
        facilitator_lambda = lambda_.Function(
            scope,
            "AsyncFacilitator",
            handler="refreezer.application.handlers.async_facilitator_handler",
            code=lambda_.Code.from_asset("source"),
            runtime=lambda_.Runtime.PYTHON_3_9,
            memory_size=256,
        )

        facilitator_lambda.add_event_source(
            DynamoEventSource(
                async_facilitator_table,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
            )
        )
        facilitator_lambda.add_environment(
            "DDB_TABLE_NAME", async_facilitator_table.table_name
        )

        topic.add_subscription(LambdaSubscription(facilitator_lambda))

        outputs[OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME] = CfnOutput(
            scope,
            OutputKeys.ASYNC_FACILITATOR_LAMBDA_NAME,
            value=facilitator_lambda.function_name,
        )

        async_facilitator_table.grant(
            facilitator_lambda, *["dynamodb:Query", "dynamodb:PutItem"]
        )
        async_facilitator_table.grant_stream(
            facilitator_lambda,
            *[
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams",
            ],
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

        assert isinstance(async_facilitator_table.node.default_child, CfnElement)
        async_facilitator_table_logical_id = Stack.of(scope).get_logical_id(
            async_facilitator_table.node.default_child
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
