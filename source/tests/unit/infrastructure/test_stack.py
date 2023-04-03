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


def test_glacier_retrieval_table_created_with_cfn_output(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["GlacierObjectRetrieval"]
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::DynamoDB::Table",
        props={
            "Properties": {
                "KeySchema": [
                    {
                        "AttributeName": "pk",
                        "KeyType": "HASH",
                    },
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                ],
            },
        },
    )
    template.has_output(
        OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME,
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
        props={},
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
                    {
                        "Action": "SNS:Publish",
                        "Condition": {
                            "StringEquals": {
                                "AWS:SourceOwner": {"Ref": "AWS::AccountId"}
                            }
                        },
                        "Effect": "Allow",
                        "Principal": {"Service": "glacier.amazonaws.com"},
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
    table_logical_id = get_logical_id(stack, ["AsyncFacilitatorTable"])
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
                                r'"GetInventoryInitiateJob":{"Next":"AsyncFacilitatorDynamoDBPut","Type":"Task","Parameters":{"AccountId":"'
                            ),
                            {"Ref": "AWS::AccountId"},
                            assertions.Match.string_like_regexp(
                                r'","JobParameters":{"Type":"inventory-retrieval","Description.\$":"\$.description","Format":"CSV","SnsTopic":"'
                            ),
                            {"Ref": topic_logical_id},
                            assertions.Match.string_like_regexp(
                                r'"},"VaultName.\$":"\$.vault_name"},"Resource":"arn:aws:states:::aws-sdk:glacier:initiateJob"},'
                                r'"AsyncFacilitatorDynamoDBPut":{"Next":"GenerateChunkArrayLambda","Type":"Task","Parameters":{"TableName":"'
                            ),
                            {"Ref": table_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","Item":{"task_token":{"S.\$":"\$\$.Task.Token"},"job_id":{"S.\$":"\$.JobId"},"start_timestamp":{"S.\$":"\$\$.Execution.StartTime"}}},'
                                r'"Resource":"arn:aws:states:::aws-sdk:dynamodb:putItem.waitForTaskToken"},'
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
                                r'"IntervalSeconds":\d+,"MaxAttempts":\d+,"BackoffRate":\d+}],"Type":"Task","Resource":"'
                            ),
                            {"Fn::GetAtt": [inventory_lambda_logical_id, "Arn"]},
                            assertions.Match.string_like_regexp(
                                r'}}},"ItemsPath":"\$.body"},'
                                r'"GlueOrderArchives":{"Type":"Pass","Next":"InventoryValidationLambda"},'
                                r'"InventoryValidationLambda":{"Type":"Pass","End":true}}}'
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


def test_initiate_retrieval_step_function_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["InitiateRetrievalStateMachine"]
    logical_id = get_logical_id(stack, resources_list)
    # TODO: Add Assertion for Initiate Retrieval step function DefinitionString

    template.has_output(
        OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN,
        {"Value": {"Ref": logical_id}},
    )


def test_retrieve_archive_step_function_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["RetrieveArchiveStateMachine"]
    logical_id = get_logical_id(stack, resources_list)
    # TODO: Add Assertion for Retrieve Archive step function DefinitionString

    template.has_output(
        OutputKeys.RETRIEVE_ARCHIVE_STATE_MACHINE_ARN,
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
                "MemorySize": 1536,
                "Timeout": 900,
            },
        },
    )

    template.has_output(
        OutputKeys.CHUNK_RETRIEVAL_LAMBDA_ARN,
        {"Value": {"Ref": logical_id}},
    )


def test_chunk_validation_lambda_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["ChunkValidation"]
    logical_id = get_logical_id(stack, resources_list)
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::Lambda::Function",
        props={
            "Properties": {
                "Handler": "refreezer.application.handlers.chunk_validation_lambda_handler",
                "Runtime": "python3.9",
                "MemorySize": 128,
                "Timeout": 180,
            },
        },
    )

    template.has_output(
        OutputKeys.CHUNK_VALIDATION_LAMBDA_ARN,
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


def test_facilitator_lambda_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    match = assertions.Match()

    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Code": {
                "S3Bucket": {"Fn::Sub": match.any_value()},
                "S3Key": match.any_value(),
            },
            "Role": {
                "Fn::GetAtt": [
                    match.string_like_regexp("AsyncFacilitatorServiceRole*"),
                    "Arn",
                ]
            },
            "Handler": "refreezer.application.handlers.async_facilitator_handler",
            "MemorySize": 256,
            "Runtime": "python3.9",
        },
    )


def test_facilitator_lambda_with_dynamoDb_event_source(
    template: assertions.Template,
) -> None:
    match = assertions.Match()
    template.has_resource_properties(
        "AWS::Lambda::EventSourceMapping",
        {
            "FunctionName": {"Ref": match.any_value()},
        },
    )


def test_facilitator_default_policy(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    match = assertions.Match()
    db_resource_name = ["AsyncFacilitatorTable"]
    facilitator_table_logical_id = get_logical_id(stack, db_resource_name)
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": match.array_with(
                    [
                        {
                            "Action": ["dynamodb:Query", "dynamodb:PutItem"],
                            "Effect": "Allow",
                            "Resource": [
                                {"Fn::GetAtt": [facilitator_table_logical_id, "Arn"]},
                                {"Ref": match.any_value()},
                            ],
                        },
                        {
                            "Action": [
                                "dynamodb:DescribeStream",
                                "dynamodb:GetRecords",
                                "dynamodb:GetShardIterator",
                                "dynamodb:ListStreams",
                            ],
                            "Effect": "Allow",
                            "Resource": {
                                "Fn::GetAtt": [
                                    facilitator_table_logical_id,
                                    "StreamArn",
                                ]
                            },
                        },
                    ]
                )
            }
        },
    )


def test_glue_job_created(stack: RefreezerStack, template: assertions.Template) -> None:
    inventory_bucket_logical_id = get_logical_id(stack, ["InventoryBucket"])
    resources = template.find_resources(
        type="AWS::Glue::Job",
        props={
            "Properties": {
                "Command": {
                    "ScriptLocation": {
                        "Fn::Join": [
                            "",
                            [
                                "s3://",
                                {"Ref": inventory_bucket_logical_id},
                                "/scripts/inventory_sort_script.py",
                            ],
                        ]
                    }
                },
            }
        },
    )
    assert 1 == len(resources)


def test_glue_job_role_created(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    inventory_bucket_logical_id = get_logical_id(stack, ["InventoryBucket"])
    match = assertions.Match()
    template.has_resource(
        "AWS::IAM::Role",
        {
            "Properties": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "glue.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                },
                "ManagedPolicyArns": [
                    {
                        "Fn::Join": [
                            "",
                            [
                                "arn:",
                                {"Ref": "AWS::Partition"},
                                ":iam::aws:policy/service-role/AWSGlueServiceRole",
                            ],
                        ]
                    }
                ],
                "Policies": [
                    {
                        "PolicyDocument": {
                            "Statement": [
                                {
                                    "Action": ["s3:PutObject", "s3:GetObject"],
                                    "Effect": "Allow",
                                    "Resource": {
                                        "Fn::Join": [
                                            "",
                                            [
                                                {
                                                    "Fn::GetAtt": [
                                                        inventory_bucket_logical_id,
                                                        "Arn",
                                                    ]
                                                },
                                                "/*",
                                            ],
                                        ]
                                    },
                                }
                            ],
                            "Version": match.any_value(),
                        },
                        "PolicyName": "GlueS3Policy",
                    }
                ],
            },
        },
    )
