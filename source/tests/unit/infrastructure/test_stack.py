"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Any, List

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
    resources_list: List[str],
    cfn_type: str,
    props: Any,
) -> None:
    resources = template.find_resources(type=cfn_type, props=props)
    assert 1 == len(resources)
    assert get_logical_id(stack, resources_list) in resources


def get_logical_id(stack: RefreezerStack, resources_list: List[str]) -> str:
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
    glue_order_job_logical_id = get_logical_id(stack, ["GlueOrderingJob"])
    glue_job_role_logical_id = get_logical_id(stack, ["GlueJobRole"])
    inventory_bucket_logical_id = get_logical_id(stack, ["InventoryBucket"])
    inventory_validate_logical_id = get_logical_id(
        stack, ["InventoryValidateMultipartUpload"]
    )
    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::StepFunctions::StateMachine",
        props={
            "Properties": {
                "RoleArn": {"Fn::GetAtt": [assertions.Match.any_value(), "Arn"]},
                "DefinitionString": {
                    "Fn::Join": [
                        "",
                        [
                            assertions.Match.string_like_regexp(
                                r'{"StartAt":"Provided Inventory\?",'
                                r'"States":{"Provided Inventory\?":{"Type":"Choice","Choices":\['
                                r'{"Variable":"\$.provided_inventory","StringEquals":"YES","Next":"GlueJobAutogenerateEtl"}\],'
                                r'"Default":"GetInventoryInitiateJob"},'
                                r'"GetInventoryInitiateJob":{"Next":"AsyncFacilitatorDynamoDBPut","Type":"Task","Parameters":{"AccountId":"'
                            ),
                            {"Ref": "AWS::AccountId"},
                            assertions.Match.string_like_regexp(
                                r'","JobParameters":{"Type":"inventory-retrieval","Description.\$":"\$.description","Format":"CSV","SnsTopic":"'
                            ),
                            {"Ref": topic_logical_id},
                            assertions.Match.string_like_regexp(
                                r'"},"VaultName.\$":"\$.vault_name"},"ResultPath":"\$.initiate_job_result","Resource":"arn:aws:states:::aws-sdk:glacier:initiateJob"},'
                                r'"AsyncFacilitatorDynamoDBPut":{"Next":"GenerateChunkArrayLambda","Type":"Task","Parameters":{"TableName":"'
                            ),
                            {"Ref": table_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","Item":{"task_token":{"S.\$":"\$\$.Task.Token"},"job_id":{"S.\$":"\$.initiate_job_result.JobId"},"start_timestamp":{"S.\$":"\$\$.Execution.StartTime"}}},'
                                r'"ResultPath":"\$.async_ddb_put_result","Resource":"arn:aws:states:::aws-sdk:dynamodb:putItem.waitForTaskToken"},'
                                r'"GenerateChunkArrayLambda":{"Next":"InitiateInventoryMultipartUpload","Retry":\[{"ErrorEquals":\["Lambda.ServiceException","Lambda.AWSLambdaException","Lambda.SdkClientException"\],'
                                r'"IntervalSeconds":\d+,"MaxAttempts":\d+,"BackoffRate":\d+}\],'
                                r'"Type":"Task","ResultPath":"\$.chunking_result","Resource":"'
                            ),
                            {
                                "Fn::GetAtt": [
                                    inventory_chunk_determination_logical_id,
                                    "Arn",
                                ]
                            },
                            assertions.Match.string_like_regexp(
                                r'","Parameters":{"InventorySize.\$":"\$.async_ddb_put_result.job_result.InventorySizeInBytes","MaximumInventoryRecordSize":\d+,"ChunkSize":\d+}},.*'
                            ),
                            {"Ref": "AWS::Partition"},
                            assertions.Match.string_like_regexp(
                                r'aws-sdk:s3:createMultipartUpload","Parameters":{"Bucket"'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'"ContentType":"text/csv","Key":"inventory.csv"}},'
                                r'"InventoryChunkRetrievalDistributedMap":{"Next":"ValidateMultipartUploadLambdaTask",'
                                r'"Type":"Map","ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},'
                                r'"StartAt":"InventoryChunkDownloadLambda","States":{"InventoryChunkDownloadLambda":{"End":true,"Retry":\[{"ErrorEquals":\["Lambda.ServiceException","Lambda.AWSLambdaException","Lambda.SdkClientException"\],'
                                r'"IntervalSeconds":\d+,"MaxAttempts":\d+,"BackoffRate":\d+}\],'
                                r'"Type":"Task","Resource"'
                            ),
                            {"Fn::GetAtt": [inventory_lambda_logical_id, "Arn"]},
                            assertions.Match.string_like_regexp(
                                r'"}}},"ItemSelector":{"JobId.\$":"\$.initiate_job_result.JobId","VaultName.\$":"\$.vault_name",'
                                r'"ByteRange.\$":"\$\$.Map.Item.Value","S3DestinationBucket":"'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","S3DestinationKey.\$":"States.Format\(\'{}/inventory.csv\', \$.workflow_run\)",'
                                r'"UploadId.\$":"\$.multipart_upload_result.UploadId".*'
                                r'"ValidateMultipartUploadLambdaTask":{"Next":"GlueJobAutogenerateEtl","Retry":\[{"ErrorEquals":\["Lambda.ServiceException","Lambda.AWSLambdaException","Lambda.SdkClientException"\],.*'
                                r'"IntervalSeconds":\d+,"MaxAttempts":\d+,"BackoffRate":\d+}\],'
                                r'"Type":"Task","Resource":"'
                            ),
                            {
                                "Fn::GetAtt": [
                                    inventory_validate_logical_id,
                                    "Arn",
                                ]
                            },
                            assertions.Match.string_like_regexp(
                                r'"},"GlueJobAutogenerateEtl":{"Next":"GlueStartJobRun","Type":"Task","Resource":"arn:'
                            ),
                            {"Ref": "AWS::Partition"},
                            assertions.Match.string_like_regexp(
                                r':states:::aws-sdk:glue:updateJob","Parameters":{"JobName":"'
                            ),
                            {"Ref": glue_order_job_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","JobUpdate":{"GlueVersion":"3.0","Role":"'
                            ),
                            {"Fn::GetAtt": [glue_job_role_logical_id, "Arn"]},
                            assertions.Match.string_like_regexp(
                                r'","ExecutionProperty":{"MaxConcurrentRuns":\d+},"CodeGenConfigurationNodes":{"node-1":{"S3CsvSource":{"Name":"S3 bucket","Paths":\["s3://'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'"],"QuoteChar":"quote","Separator":"comma","Recurse":true,"WithHeader":true,"Escaper":"","OutputSchemas":\[{"Columns":\[{"Name":"ArchiveId","Type":"string"},{"Name":"ArchiveDescription","Type":"string"},{"Name":"CreationDate","Type":"string"},{"Name":"Size","Type":"string"},{"Name":"SHA256TreeHash","Type":"string"}]}]}},"node-2":{"SparkSQL":{"Name":"sql","Inputs":\["node\-1"],"SqlQuery":"select \* from myDataSource ORDER BY CreationDate ASC;","SqlAliases":\[{"From":"node-1","Alias":"myDataSource"}]}},"node-3":{"S3DirectTarget":{"Inputs":\["node-2"],"PartitionKeys":\[],"Compression":"none","Format":"csv","SchemaChangePolicy":{"EnableUpdateCatalog":false},"Path":"s3://'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'/sorted_inventory/","Name":"S3 bucket"}},"node-4":{"CustomCode":{"Inputs":\["node\-1","node\-2"],"ClassName":"Validation","Code":"\\nnode_inputs = list\(dfc\.values\(\)\)\\nassert node_inputs\[0].toDF\(\).count\(\) == node_inputs\[1].toDF\(\).count\(\)\\n","Name":"Validation"}}},"Command":{"Name":"glueetl","ScriptLocation":"s3://'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'/workflow_run_id/scripts/inventory_sort_script.py","PythonVersion":"3"}}}},"GlueStartJobRun":{"End":true,"Type":"Task","Resource":"arn:'
                            ),
                            {"Ref": "AWS::Partition"},
                            assertions.Match.string_like_regexp(
                                r':states:::glue:startJobRun","Parameters":{"JobName":"'
                            ),
                            {"Ref": glue_order_job_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","Timeout":\d+,"NotificationProperty":{"NotifyDelayAfter":\d+}}}}}'
                            ),
                        ],
                    ]
                },
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
        glacier_object_table_logical_id = get_logical_id(
            stack, ["GlacierObjectRetrieval"]
        )
        inventory_bucket_logical_id = get_logical_id(stack, ["InventoryBucket"])
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
                                    r'{"StartAt":"InitiateRetrievalDistributedMap","States":{"InitiateRetrievalDistributedMap":{"End":true,"Type":"Map",'
                                    r'"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},'
                                    r'"StartAt":"InitiateRetrievalInnerDistributedMap","States":{"InitiateRetrievalInnerDistributedMap":{"End":true,"Type":"Map",'
                                    r'"ItemProcessor":{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},'
                                    r'"StartAt":"InitiateRetrievalInitiateJob","States":{"InitiateRetrievalInitiateJob":{"Type":"Pass","Next":"InitiateRetrievalWorkflowDynamoDBPut"},'
                                    r'"InitiateRetrievalWorkflowDynamoDBPut":{"End":true,"Type":"Task","Parameters":{"TableName":"'
                                ),
                                {"Ref": glacier_object_table_logical_id},
                                assertions.Match.string_like_regexp(
                                    r'","Item":{"pk":{"S":"IR:\$.ArchiveId"},"sk":{"S":"meta"},"job_id":{"S":"\$.JobId"},'
                                    r'"start_timestamp":{"S":"\$\$.Execution.StartTime"}}},"Resource":"arn:aws:states:::aws-sdk:dynamodb:putItem"}}},'
                                    r'"MaxConcurrency":1,"ItemSelector":{"bucket.\$":"\$.bucket","key.\$":"\$.item.Key","item.\$":"\$\$.Map.Item.Value"},'
                                    r'"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"'
                                ),
                                {"Ref": inventory_bucket_logical_id},
                                assertions.Match.string_like_regexp(
                                    r'","Prefix.\$":"States.Format(\'{}/initiate_retrieval_inner_distributed_map_output\', \$.workflow_run)"}},'
                                    r'"ResultPath":"\$.map_result","ItemReader":{"Resource":"arn:aws:states:::s3:getObject",'
                                    r'"ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":"FIRST_ROW"},'
                                    r'"Parameters":{"Bucket.\$":"\$.bucket","Key.\$":"\$.item.Key"}}}}},"MaxConcurrency":1,"ItemSelector":{"bucket":"'
                                ),
                                {"Ref": inventory_bucket_logical_id},
                                assertions.Match.string_like_regexp(
                                    r'","workflow_run.\$":"\$.workflow_run","item.\$":"\$\$.Map.Item.Value"},'
                                    r'"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"'
                                ),
                                {"Ref": inventory_bucket_logical_id},
                                assertions.Match.string_like_regexp(
                                    r'","Prefix.\$":"States.Format(\'{}/initiate_retrieval_distributed_map_output\', \$.workflow_run)"}},'
                                    r'"ResultPath":"\$.map_result",'
                                    r'"ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","Parameters":{"Bucket":"'
                                ),
                                {"Ref": inventory_bucket_logical_id},
                                assertions.Match.string_like_regexp(
                                    r'","Prefix.\$":"States.Format(\'{}/sorted_inventory\', \$.workflow_run)"}}}}}'
                                ),
                            ],
                        ]
                    }
                }
            },
        )

        template.has_output(
            OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN,
            {"Value": {"Ref": logical_id}},
        )


def test_retrieve_archive_step_machine(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["RetrieveArchiveStateMachine"]
    glacier_logical_id = get_logical_id(stack, ["GlacierObjectRetrieval"])
    inventory_bucket_logical_id = get_logical_id(stack, ["InventoryBucket"])
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
                                r'{"StartAt":"RetrieveArchiveDistributedMap","States":{"RetrieveArchiveDistributedMap"'
                                r':{"Next":"RetrieveArchiveValidateLambdaTask","Type":"Map","ItemProcessor":'
                                r'{"ProcessorConfig":{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},'
                                r'"StartAt":"RetrieveArchiveInnerDistributedMap","States":{"RetrieveArchiveInnerDistributedMap"'
                                r':{"End":true,"Type":"Map","ItemProcessor":{"ProcessorConfig":'
                                r'{"Mode":"DISTRIBUTED","ExecutionType":"STANDARD"},"StartAt":'
                                r'"RetrieveArchiveDynamoDBGetJob","States":{"RetrieveArchiveDynamoDBGetJob"'
                                r':{"Next":"RetrieveArchiveDynamoDBPut","Type":"Task","Resource":"arn:'
                            ),
                            {"Ref": "AWS::Partition"},
                            assertions.Match.string_like_regexp(
                                r':states:::dynamodb:getItem","Parameters":{"Key":{"pk":{"S":"States.Format'
                                r'\(\'{}_:{}\', \$.workflow_run, \$.item.ArchiveId\)"},"sk":{"S":"meta"}},"TableName":"',
                            ),
                            {"Ref": glacier_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","ConsistentRead":false}},"RetrieveArchiveDynamoDBPut":{"Type":"Pass",'
                                r'"Next":"RetrieveArchiveStartMultipartUpload"},"RetrieveArchiveStartMultipartUpload":'
                                r'{"Type":"Pass","Next":"RetrieveArchiveGenerateChunkArrayLambda"}'
                                r',"RetrieveArchiveGenerateChunkArrayLambda":{"Type":"Pass","Parameters"'
                                r':{"chunk_array":\["0-499","500-930"\]},"Next":"RetrieveArchiveChunkDistributedMap"},'
                                r'"RetrieveArchiveChunkDistributedMap":{"Type":"Map","End":true,"Iterator":{"StartAt":'
                                r'"RetrieveArchivechunkProcessingLambdaTask","States":{"RetrieveArchivechunkProcessingLambdaTask":'
                                r'{"Type":"Pass","End":true}}},"ItemsPath":"\$.chunk_array"}}},"ItemSelector":'
                                r'{"bucket.\$":"\$.bucket","key.\$":"\$.item.Key","workflow_run.\$":"\$.workflow_run","item.\$":"\$\$.Map.Item.Value"}'
                                r',"ResultWriter":{"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","Prefix.\$":"States.Format\(\'{}/RetrieveArchiveInnerDistributedMapOutput\','
                                r' \$.workflow_run\)"}},"ResultPath":"\$.map_result","ItemReader":{"Resource":'
                                r'"arn:aws:states:::s3:getObject","ReaderConfig":{"InputType":"CSV","CSVHeaderLocation":'
                                r'"FIRST_ROW"},"Parameters":{"Bucket.\$":"\$.bucket","Key.\$":"\$.item.Key"}}}}},'
                                r'"ItemSelector":{"bucket":"'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","workflow_run.\$":"\$.workflow_run","item.\$":"\$\$.Map.Item.Value"},"ResultWriter"'
                                r':{"Resource":"arn:aws:states:::s3:putObject","Parameters":{"Bucket":"'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","Prefix.\$":"States.Format\(\'{}/RetrieveArchiveDistributedMapOutput\','
                                r' \$.workflow_run\)"}},"ResultPath":"\$.map_result","ItemReader":{"Resource":"arn:aws:states:::s3:listObjectsV2","Parameters":{"Bucket":"'
                            ),
                            {"Ref": inventory_bucket_logical_id},
                            assertions.Match.string_like_regexp(
                                r'","Prefix.\$":"States.Format\(\'{}/sorted_inventory\', \$.workflow_run\)"}}}'
                                r',"RetrieveArchiveValidateLambdaTask":{"Type":"Pass","Next":"RetrieveArchiveCloseMultipartUpload"}'
                                r',"RetrieveArchiveCloseMultipartUpload":{"Type":"Pass","End":true}}}'
                            ),
                        ],
                    ]
                }
            }
        },
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


def test_retrieve_archive_state_machine_policy(
    stack: RefreezerStack, template: assertions.Template
) -> None:
    resources_list = ["RetrieveArchiveStateMachinePolicy"]
    retrieve_archive_state_machine_logical_id = get_logical_id(
        stack, ["RetrieveArchiveStateMachine"]
    )
    retrieve_archive_state_machine_role_logical_id = get_logical_id(
        stack, ["RetrieveArchiveStateMachine", "Role"]
    )

    assert_resource_name_has_correct_type_and_props(
        stack,
        template,
        resources_list=resources_list,
        cfn_type="AWS::IAM::Policy",
        props={
            "Properties": {
                "PolicyDocument": {
                    "Statement": [
                        {
                            "Action": "states:StartExecution",
                            "Effect": "Allow",
                            "Resource": {
                                "Ref": retrieve_archive_state_machine_logical_id
                            },
                        },
                        {
                            "Action": [
                                "states:DescribeExecution",
                                "states:StopExecution",
                            ],
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
                                                retrieve_archive_state_machine_logical_id,
                                                "Name",
                                            ]
                                        },
                                        "/*",
                                    ],
                                ]
                            },
                        },
                    ]
                },
                "Roles": [{"Ref": retrieve_archive_state_machine_role_logical_id}],
            },
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
                                "/workflow_run_id/scripts/inventory_sort_script.py",
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
