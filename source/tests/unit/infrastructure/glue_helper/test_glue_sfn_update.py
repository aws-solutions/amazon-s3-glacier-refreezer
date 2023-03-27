"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest
import cdk_nag
import aws_cdk as core
import aws_cdk.assertions as assertions
from aws_cdk.aws_stepfunctions_tasks import GlueStartJobRun
from constructs import Construct

from refreezer.infrastructure.glue_helper.glue_sfn_update import GlueSfnUpdate

from refreezer.infrastructure.stack import (
    RefreezerStack,
)


@pytest.fixture
def glue_sfn_update(stack: RefreezerStack) -> GlueSfnUpdate:
    input_bucket = "test-bucket"
    s3_bucket_arn = "arn:aws:s3:::test-bucket"
    glue_job_name = "test-job"
    glue_job_arn = "arn:aws:glue:us-east-1:123456789012:job/test-job"
    glue_sfn_update = GlueSfnUpdate(
        stack, "Name", input_bucket, s3_bucket_arn, glue_job_name, glue_job_arn
    )
    return glue_sfn_update


def test_autogenerate_etl_script(glue_sfn_update: GlueSfnUpdate) -> None:
    task = glue_sfn_update.autogenerate_etl_script()
    state_json = task.to_state_json()
    resource = state_json["Resource"]
    assert state_json == {
        "End": True,
        "Type": "Task",
        "Resource": resource,
        "Parameters": {
            "JobName": "test-job",
            "JobUpdate": {
                "GlueVersion": "3.0",
                "Role": "arn:aws:glue:us-east-1:123456789012:job/test-job",
                "CodeGenConfigurationNodes": {
                    "node-1": {
                        "S3CsvSource": {
                            "Name": "S3 bucket",
                            "Paths": ["s3://test-bucket/"],
                            "QuoteChar": "quote",
                            "Separator": "comma",
                            "Recurse": True,
                            "WithHeader": True,
                            "Escaper": "",
                            "OutputSchemas": [
                                {
                                    "Columns": [
                                        {"Name": "ArchiveId", "Type": "string"},
                                        {
                                            "Name": "ArchiveDescription",
                                            "Type": "string",
                                        },
                                        {"Name": "CreationDate", "Type": "string"},
                                        {"Name": "Size", "Type": "string"},
                                        {"Name": "SHA256TreeHash", "Type": "string"},
                                    ]
                                }
                            ],
                        }
                    },
                    "node-2": {
                        "SparkSQL": {
                            "Name": "sql",
                            "Inputs": ["node-1"],
                            "SqlQuery": "select * from myDataSource ORDER BY CreationDate ASC;",
                            "SqlAliases": [{"From": "node-1", "Alias": "myDataSource"}],
                        }
                    },
                    "node-3": {
                        "S3DirectTarget": {
                            "Inputs": ["node-2"],
                            "PartitionKeys": [],
                            "Compression": "none",
                            "Format": "csv",
                            "SchemaChangePolicy": {"EnableUpdateCatalog": False},
                            "Path": "s3://test-bucket/sorted_inventory/",
                            "Name": "S3 bucket",
                        }
                    },
                    "node-4": {
                        "CustomCode": {
                            "Inputs": ["node-1", "node-2"],
                            "ClassName": "Validation",
                            "Code": """
node_inputs = list(dfc.values())
assert node_inputs[0].toDF().count() == node_inputs[1].toDF().count()
""",
                            "Name": "Validation",
                        }
                    },
                },
                "Command": {
                    "Name": "glueetl",
                    "ScriptLocation": "s3://test-bucket/scripts/inventory_sort_script.py",
                    "PythonVersion": "3",
                },
            },
        },
    }


def test_start_job(glue_sfn_update: GlueSfnUpdate) -> None:
    task = glue_sfn_update.start_job()
    state_json = task.to_state_json()
    assert isinstance(task, GlueStartJobRun)
    assert state_json["Parameters"]["JobName"] == "test-job"
