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


def test_get_inventory_step_function_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["InventoryRetrievalStateMachine"]
    logical_id = get_logical_id(stack, resources_list)
    topic_logical_id = get_logical_id(stack, ["AsyncFacilitatorTopic"])
    inventory_lambda_logical_id = get_logical_id(stack, ["InventoryChunkDownload"])
    inventory_chunk_determination_logical_id = get_logical_id(
        stack, ["InventoryChunkDetermination"]
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
                                r'{"StartAt":"Provided Inventory\?",'
                                r'"States":{"Provided Inventory\?":{"Type":"Choice","Choices":\['
                                r'{"Variable":"\$.provided_inventory","StringEquals":"YES","Next":"GlueOrderArchives"}\],'
                                r'"Default":"GetInventoryInitiateJob"},'
                                r'"GetInventoryInitiateJob":{"Next":"DynamoDBPut","Type":"Task","Parameters":{"AccountId":"'
                            ),
                            {"Ref": "AWS::AccountId"},
                            assertions.Match.string_like_regexp(
                                r'","JobParameters":{"Type":"inventory-retrieval","Description.\$":"\$.description","Format":"CSV","SnsTopic":"'
                            ),
                            {"Ref": topic_logical_id},
                            assertions.Match.string_like_regexp(
                                r'"},"VaultName.\$":"\$.vault_name"},"Resource":"arn:aws:states:::aws-sdk:glacier:initiateJob"},'
                                r'"DynamoDBPut":{"Type":"Pass","Parameters":{"InventorySize":\d+,"MaximumInventoryRecordSize":\d+,"ChunkSize":\d+},"Next":"GenerateChunkArrayLambda"},'
                                r'"GenerateChunkArrayLambda":{"Next":"DistributedMap","Retry":\[{"ErrorEquals":\["Lambda.ServiceException","Lambda.AWSLambdaException","Lambda.SdkClientException"\],'
                                r'"IntervalSeconds":\d+,"MaxAttempts":\d+,"BackoffRate":\d+}\],'
                                r'"Type":"Task","Resource":"'
                            ),
                            {
                                "Fn::GetAtt": [
                                    inventory_chunk_determination_logical_id,
                                    "Arn",
                                ]
                            },
                            assertions.Match.string_like_regexp(
                                r'"DistributedMap":{"Type":"Map","Next":"GlueOrderArchives","Iterator":{"StartAt":"InventoryChunkDownloadLambda",'
                                r'"States":{"InventoryChunkDownloadLambda":{"End":true,"Retry":\[{"ErrorEquals":\["Lambda.ServiceException","Lambda.AWSLambdaException","Lambda.SdkClientException"\],'
                                r'"IntervalSeconds":\d+,"MaxAttempts":\d+,"BackoffRate":\d+}],"Type":"Task","Resource":'
                            ),
                            {"Fn::GetAtt": [inventory_lambda_logical_id, "Arn"]},
                            assertions.Match.string_like_regexp(
                                r'"ItemsPath":"\$.body"},'
                                r'"GlueOrderArchives":{"Type":"Pass","Next":"InventoryValidationLambda"},'
                                r'"InventoryValidationLambda":{"Type":"Pass","End":true'
                            ),
                        ],
                    ]
                }
            }
        },
    )

    template.has_output(
        OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN,
        {"Value": {"Ref": logical_id}},
    )


def test_chunk_retrieval_lambda_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["ChunkRetrieval"]
    logical_id = get_logical_id(stack, resources_list)
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::Lambda::Function",
        props={
            "Properties": {
                "Handler": "refreezer.application.handlers.chunk_retrieval_lambda_handler",
                "Runtime": "python3.9",
                "MemorySize": 4096,
                "Timeout": 900,
            },
        },
    )

    template.has_output(
        OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN,
        {"Value": {"Ref": logical_id}},
    )


def test_inventory_chunk_determination_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["InventoryChunkDetermination"]
    logical_id = get_logical_id(stack, resources_list)
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::Lambda::Function",
        props={
            "Properties": {
                "Handler": "refreezer.application.handlers.inventory_chunk_lambda_handler",
                "Runtime": "python3.9",
            },
        },
    )

    template.has_output(
        OutputKeys.INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN,
        {"Value": {"Ref": logical_id}},
    )
