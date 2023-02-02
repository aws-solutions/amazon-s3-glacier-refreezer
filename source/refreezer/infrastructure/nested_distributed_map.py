"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_stepfunctions as sfn,
    aws_logs,
    aws_s3 as s3,
    CfnElement,
)
from constructs import Construct
from cdk_nag import NagSuppressions
from typing import Dict, Any


class NestedDistributedMap(Construct):
    def __init__(
        self,
        scope: Construct,
        nested_distributed_map_id: str,
        bucket: s3.Bucket,
        definition: sfn.IChainable,
        max_concurrency: int,
    ) -> None:
        super().__init__(scope, nested_distributed_map_id)

        self.bucket = bucket
        self.definition = definition
        self.max_concurrency = max_concurrency

        self.state_machine = sfn.StateMachine(
            self,
            "StateMachine",
            definition=self.create_nested_distributed_maps(),
            logs=sfn.LogOptions(
                level=sfn.LogLevel.ALL,
                destination=aws_logs.LogGroup(self, "SfnLogGroup"),
            ),
            tracing_enabled=True,
        )

        # https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html"
        self.bucket.grant_read(self.state_machine)

        self.state_machine_policy = iam.Policy(
            self,
            "StateMachinePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                    ],
                    resources=[self.state_machine.state_machine_arn],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["states:DescribeExecution", "states:StopExecution"],
                    resources=[
                        "arn:aws:states:"
                        + Stack.of(self).region
                        + ":"
                        + Stack.of(self).account
                        + ":execution:"
                        + self.state_machine.state_machine_name
                        + "/*"
                    ],
                ),
            ],
        )

        self.state_machine_policy.attach_to_role(self.state_machine.role)

        self.cdk_nag_suppression()

    def create_nested_distributed_maps(self) -> sfn.CustomState:
        # Inner CSV distributed Map
        csv_item_reader_parameters = {"Bucket.$": "$.bucket", "Key.$": "$.item.Key"}
        csv_item_selector = {
            "bucket.$": "$.bucket",
            "key.$": "$.item.Key",
            "item.$": "$$.Map.Item.Value",
        }
        csv_result_path = "$.map_result"
        csv_item_reader_resource = "arn:aws:states:::s3:getObject"
        csv_reader_config = {
            "InputType": "CSV",
            "CSVHeaderLocation": "FIRST_ROW",
        }

        process_csv_file_distributed_map = self.create_distributed_map(
            scope=self,
            distributed_map_id="ProcessCSVFile",
            definition=self.definition,
            max_concurrency=self.max_concurrency,
            item_reader_resource=csv_item_reader_resource,
            reader_config=csv_reader_config,
            item_reader_parameters=csv_item_reader_parameters,
            item_selector=csv_item_selector,
            result_selector={},
            result_path=csv_result_path,
        )

        # Main S3 distributed map
        s3_item_reader_parameters = {"Bucket": self.bucket.bucket_name}
        s3_item_selector = {
            "bucket": self.bucket.bucket_name,
            "item.$": "$$.Map.Item.Value",
        }
        s3_result_path = "$.map_result"
        s3_item_reader_resource = "arn:aws:states:::s3:listObjectsV2"
        s3_reader_config: Dict[str, Any] = {}

        process_s3_objects_distributed_map = self.create_distributed_map(
            scope=process_csv_file_distributed_map,
            distributed_map_id="ProcessS3Objects",
            definition=process_csv_file_distributed_map,
            max_concurrency=self.max_concurrency,
            item_reader_resource=s3_item_reader_resource,
            reader_config=s3_reader_config,
            item_reader_parameters=s3_item_reader_parameters,
            item_selector=s3_item_selector,
            result_selector={},
            result_path=s3_result_path,
        )

        return process_s3_objects_distributed_map

    def create_distributed_map(
        self,
        scope: Construct,
        distributed_map_id: str,
        definition: sfn.IChainable,
        max_concurrency: int,
        item_reader_resource: str,
        reader_config: Dict[str, Any],
        item_reader_parameters: Dict[str, Any],
        item_selector: Dict[str, Any],
        result_selector: Dict[str, Any],
        result_path: str,
    ) -> sfn.CustomState:
        map = sfn.Map(scope, "InlineMap")
        map.iterator(definition)
        map_iterator = map.to_state_json()["Iterator"]

        state_json: Dict[str, Any]
        state_json = {
            "Type": "Map",
            "MaxConcurrency": max_concurrency,
            "ItemReader": {
                "Resource": item_reader_resource,
                "ReaderConfig": reader_config,
                "Parameters": item_reader_parameters,
            },
            "ItemSelector": item_selector,
            "ItemProcessor": {
                "ProcessorConfig": {
                    "Mode": "DISTRIBUTED",
                    "ExecutionType": "STANDARD",
                },
            },
            "ResultSelector": result_selector,
            "ResultPath": result_path,
        }
        state_json["ItemProcessor"].update(map_iterator)
        return sfn.CustomState(scope, distributed_map_id, state_json=state_json)

    def cdk_nag_suppression(self) -> None:
        assert isinstance(self.bucket.node.default_child, CfnElement)
        bucket_logical_id = Stack.of(self).get_logical_id(
            self.bucket.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            self.state_machine.role.node.find_child("DefaultPolicy").node.find_child(
                "Resource"
            ),
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "Allow resource wildcard permission for Step Function logging",
                    "appliesTo": ["Resource::*"],
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy for reading a file as dataset in a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        "Resource::<" + bucket_logical_id + ".Arn>/*",
                        "Action::s3:GetBucket*",
                        "Action::s3:GetObject*",
                        "Action::s3:List*",
                    ],
                },
            ],
        )
        assert isinstance(self.state_machine.node.default_child, CfnElement)
        state_machine_logical_id = Stack.of(self).get_logical_id(
            self.state_machine.node.default_child
        )
        NagSuppressions.add_resource_suppressions(
            self.state_machine_policy,
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": "IAM policy for running a Distributed Map state. https://docs.aws.amazon.com/step-functions/latest/dg/iam-policies-eg-dist-map.html",
                    "appliesTo": [
                        "Resource::arn:aws:states:<AWS::Region>:<AWS::AccountId>:execution:<"
                        + state_machine_logical_id
                        + ".Name>/*"
                    ],
                }
            ],
        )
