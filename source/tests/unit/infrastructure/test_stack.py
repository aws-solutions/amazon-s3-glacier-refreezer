"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import typing

import aws_cdk as core
import aws_cdk.assertions as assertions
import cdk_nag
import pytest

from refreezer.infrastructure.stack import (
    RefreezerStack,
    OutputKeys,
)


@pytest.fixture
def stack() -> RefreezerStack:
    app = core.App()
    stack = RefreezerStack(app, "refreezer")
    core.Aspects.of(stack).add(
        cdk_nag.AwsSolutionsChecks(log_ignores=True, verbose=True)
    )
    return stack


@pytest.fixture
def template(stack: RefreezerStack) -> assertions.Template:
    return assertions.Template.from_stack(stack)


def assert_resource_name_has_correct_type_and_props(
    stack: RefreezerStack,
    template: assertions.Template,
    resources_list: typing.List[str],
    cfn_type: str,
    props: typing.Any,
) -> None:
    resources = template.find_resources(type=cfn_type, props=props)
    assert 1 == len(resources)
    assert get_logical_id(stack, resources_list) in resources


def get_logical_id(stack: RefreezerStack, resources_list: typing.List[str]) -> str:
    node = stack.node
    for resource in resources_list:
        node = node.find_child(resource).node
    cfnElement = node.default_child
    assert isinstance(cfnElement, core.CfnElement)
    return stack.get_logical_id(cfnElement)


def test_cdk_app() -> None:
    import refreezer.app

    refreezer.app.main()


def test_cdk_nag(stack: RefreezerStack) -> None:
    assertions.Annotations.from_stack(stack).has_no_error(
        "*", assertions.Match.any_value()
    )
    assertions.Annotations.from_stack(stack).has_no_warning(
        "*", assertions.Match.any_value()
    )


def test_job_tracking_table_created_with_cfn_output(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["AsyncFacilitatorTable"]
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::DynamoDB::Table",
        props={
            "Properties": {
                "KeySchema": [
                    {
                        "AttributeName": "job_id",
                        "KeyType": "HASH",
                    }
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "job_id", "AttributeType": "S"}
                ],
            },
        },
    )

    template.has_output(
        OutputKeys.ASYNC_FACILITATOR_TABLE_NAME,
        {"Value": {"Ref": get_logical_id(stack, resources_list)}},
    )


def test_cfn_outputs_logical_id_is_same_as_key(stack: RefreezerStack) -> None:
    """
    The outputs are used to build environment variables to pass in to lambdas,
    so we need to ensure the resource name is the same as the resulting logical
    id. Outputs have non-alphanumeric characters removed, like '-', so this
    makes sure they aren't part of the resource name.
    """
    for key, output in stack.outputs.items():
        assert key == stack.get_logical_id(output)


def test_glacier_sns_topic_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["AsyncFacilitatorTopic"]
    logical_id = get_logical_id(stack, resources_list)
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::SNS::Topic",
        props={"Properties": {"KmsMasterKeyId": assertions.Match.any_value()}},
    )

    template.has_output(
        OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN,
        {"Value": {"Ref": logical_id}},
    )

    template.has_resource_properties(
        "AWS::SNS::TopicPolicy",
        {
            "PolicyDocument": {
                "Statement": [
                    {
                        "Action": "sns:Publish",
                        "Condition": {"Bool": {"aws:SecureTransport": False}},
                        "Effect": "Deny",
                        "Principal": {"AWS": "*"},
                        "Resource": {"Ref": logical_id},
                    },
                ],
            },
            "Topics": [
                {
                    "Ref": logical_id,
                }
            ],
        },
    )


def test_buckets_created(stack: RefreezerStack, template: assertions.Template) -> None:
    resources = template.find_resources(
        type="AWS::S3::Bucket",
        props={
            "Properties": {
                "BucketEncryption": {
                    "ServerSideEncryptionConfiguration": [
                        {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                    ]
                },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True,
                },
                "VersioningConfiguration": {"Status": "Enabled"},
            }
        },
    )
    assert 2 == len(resources)
    assert get_logical_id(stack, ["OutputBucket"]) in resources
    assert get_logical_id(stack, ["InventoryBucket"]) in resources

    template.has_resource_properties(
        "AWS::S3::BucketPolicy",
        {
            "Bucket": {"Ref": logical_id},
            "PolicyDocument": {
                "Statement": [
                    {
                        "Action": "s3:*",
                        "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                        "Effect": "Deny",
                        "Principal": {"AWS": "*"},
                        "Resource": [
                            {"Fn::GetAtt": [logical_id, "Arn"]},
                            {
                                "Fn::Join": [
                                    "",
                                    [
                                        {
                                            "Fn::GetAtt": [
                                                logical_id,
                                                "Arn",
                                            ]
                                        },
                                        "/*",
                                    ],
                                ]
                            },
                        ],
                    },
                ],
            },
        },
    )


def test_step_function_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["InitiateRetrievalNestedDistributedMap", "StateMachine"]
    state_machine_logical_id = get_logical_id(stack, resources_list)
    bucket_logical_id = get_logical_id(stack, ["InventoryBucket"])
    log_group_logical_id = get_logical_id(
        stack, ["InitiateRetrievalNestedDistributedMap", "SfnLogGroup"]
    )
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::StepFunctions::StateMachine",
        props={
            "Properties": {
                "DefinitionString": {
                    "Fn::Join": [
                        "",
                        [
                            assertions.Match.string_like_regexp(
                                (
                                    r'{"StartAt":"ProcessS3Objects","States":{"ProcessS3Objects":{"End":true,"Type":"Map","MaxConcurrency":[0-9]+,'
                                    r'"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2",'
                                    r'"ReaderConfig":{},"Parameters":{"Bucket":"'
                                )
                            ),
                            {"Ref": bucket_logical_id},
                            '"}},"ItemSelector":{"bucket":"',
                            {"Ref": bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                (
                                    r'","item.\$":"\$\$.Map.Item.Value"},'
                                    r'"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},'
                                    r'"StartAt":"ProcessCSVFile","States":{"ProcessCSVFile":{"End":true,"Type":"Map","MaxConcurrency":[0-9]+,'
                                    r'"ItemReader":{"Resource":"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"FIRST_ROW"},'
                                    r'"Parameters":{"Bucket.\$":"\$.bucket","Key.\$":"\$.item.Key"}},'
                                    r'"ItemSelector":{"bucket.\$":"\$.bucket","key.\$":"\$.item.Key","item.\$":"\$\$.Map.Item.Value"},'
                                    r'"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},'
                                    r'"StartAt":".+","States":[\s\{]*(\{.*\})[\s\}]*},'
                                    r'"ResultSelector":{},"ResultPath":"\$.map_result"}}},'
                                    r'"ResultSelector":{},"ResultPath":"\$.map_result"}}}'
                                )
                            ),
                        ],
                    ]
                },
                "LoggingConfiguration": {
                    "Destinations": [
                        {
                            "CloudWatchLogsLogGroup": {
                                "LogGroupArn": {
                                    "Fn::GetAtt": [log_group_logical_id, "Arn"]
                                }
                            }
                        }
                    ],
                    "Level": "ALL",
                },
                "TracingConfiguration": {"Enabled": True},
            }
        },
    )

    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        {"RetentionInDays": assertions.Match.any_value()},
    )

    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": [
                    {
                        "Action": [
                            "logs:CreateLogDelivery",
                            "logs:GetLogDelivery",
                            "logs:UpdateLogDelivery",
                            "logs:DeleteLogDelivery",
                            "logs:ListLogDeliveries",
                            "logs:PutResourcePolicy",
                            "logs:DescribeResourcePolicies",
                            "logs:DescribeLogGroups",
                        ],
                        "Effect": "Allow",
                        "Resource": "*",
                    },
                    {
                        "Action": [
                            "xray:PutTraceSegments",
                            "xray:PutTelemetryRecords",
                            "xray:GetSamplingRules",
                            "xray:GetSamplingTargets",
                        ],
                        "Effect": "Allow",
                        "Resource": "*",
                    },
                    {
                        "Action": ["s3:GetObject*", "s3:GetBucket*", "s3:List*"],
                        "Effect": "Allow",
                        "Resource": [
                            {"Fn::GetAtt": [bucket_logical_id, "Arn"]},
                            {
                                "Fn::Join": [
                                    "",
                                    [{"Fn::GetAtt": [bucket_logical_id, "Arn"]}, "/*"],
                                ]
                            },
                        ],
                    },
                ],
            },
        },
    )

    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": [
                    {
                        "Action": "states:StartExecution",
                        "Effect": "Allow",
                        "Resource": {"Ref": state_machine_logical_id},
                    },
                    {
                        "Action": ["states:DescribeExecution", "states:StopExecution"],
                        "Effect": "Allow",
                        "Resource": {
                            "Fn::Join": [
                                "",
                                [
                                    "arn:aws:states:",
                                    {"Ref": "AWS::Region"},
                                    ":",
                                    {"Ref": "AWS::AccountId"},
                                    ":execution:",
                                    {
                                        "Fn::GetAtt": [
                                            state_machine_logical_id,
                                            "Name",
                                        ]
                                    },
                                    "/*",
                                ],
                            ]
                        },
                    },
                ],
            }
        },
    )
