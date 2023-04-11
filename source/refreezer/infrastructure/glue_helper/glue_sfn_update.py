"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from aws_cdk import Duration

PYTHON_VERSION = "3"
GLUE_VERSION = "3.0"
JOB_COMMAND_NAME = "glueetl"
SQL_QUERY = "select * from myDataSource ORDER BY CreationDate ASC;"
OUTPUT_BUCKET_PREFIX = "sorted_inventory/"
CSV_FILE_COLUMNS = [
    {"Name": "ArchiveId", "Type": "string"},
    {"Name": "ArchiveDescription", "Type": "string"},
    {"Name": "CreationDate", "Type": "string"},
    {"Name": "Size", "Type": "string"},
    {"Name": "SHA256TreeHash", "Type": "string"},
]
VALIDATION_CODE = """
node_inputs = list(dfc.values())
assert node_inputs[0].toDF().count() == node_inputs[1].toDF().count()
"""
CUSTOM_CODE_NAME = "Validation"


class GlueSfnUpdate(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        s3_bucket_name: str,
        s3_bucket_arn: str,
        glue_job_name: str,
        glue_job_arn: str,
        glue_script_location: str,
        glue_max_concurent_runs: int = 1,
    ) -> None:
        super().__init__(scope, id)

        self.s3_bucket_name = s3_bucket_name
        self.s3_bucket_arn = s3_bucket_arn
        self.glue_job_name = glue_job_name
        self.glue_job_arn = glue_job_arn
        self.glue_script_location = glue_script_location
        self.glue_max_concurent_runs = glue_max_concurent_runs

    def autogenerate_etl_script(self) -> tasks.CallAwsService:
        graph_workflow = {
            "node-1": {
                "S3CsvSource": {
                    "Name": "S3 bucket",
                    "Paths": [f"s3://{self.s3_bucket_name}/"],
                    "QuoteChar": "quote",
                    "Separator": "comma",
                    "Recurse": True,
                    "WithHeader": True,
                    "Escaper": "",
                    "OutputSchemas": [{"Columns": CSV_FILE_COLUMNS}],
                },
            },
            "node-2": {
                "SparkSQL": {
                    "Name": "sql",
                    "Inputs": ["node-1"],
                    "SqlQuery": SQL_QUERY,
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
                    "Path": f"s3://{self.s3_bucket_name}/{OUTPUT_BUCKET_PREFIX}",
                    "Name": "S3 bucket",
                }
            },
            "node-4": {
                "CustomCode": {
                    "Inputs": ["node-1", "node-2"],
                    "ClassName": CUSTOM_CODE_NAME,
                    "Code": VALIDATION_CODE,
                    "Name": CUSTOM_CODE_NAME,
                }
            },
        }

        return tasks.CallAwsService(
            self,
            "GlueJobAutogenerateEtl",
            service="glue",
            action="updateJob",
            parameters={
                "JobName": self.glue_job_name,
                "JobUpdate": {
                    "GlueVersion": GLUE_VERSION,
                    "Role": self.glue_job_arn,
                    "ExecutionProperty": {
                        "MaxConcurrentRuns": self.glue_max_concurent_runs
                    },
                    "CodeGenConfigurationNodes": graph_workflow,
                    "Command": {
                        "Name": JOB_COMMAND_NAME,
                        "ScriptLocation": f"s3://{self.s3_bucket_name}/{self.glue_script_location}",
                        "PythonVersion": PYTHON_VERSION,
                    },
                },
            },
            iam_resources=[self.glue_job_arn, self.s3_bucket_arn],
            iam_action="iam:PassRole",
        )

    def start_job(self) -> tasks.GlueStartJobRun:
        return tasks.GlueStartJobRun(
            self,
            "GlueStartJobRun",
            glue_job_name=self.glue_job_name,
            timeout=Duration.minutes(30),
            notify_delay_after=Duration.minutes(5),
        )
