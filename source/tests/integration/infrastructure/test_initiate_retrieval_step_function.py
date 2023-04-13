"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import boto3
import pytest
import json
import random
import string
from datetime import datetime, timedelta
import hashlib
import binascii
import csv
import io

from tests.integration.infrastructure import sfn_util
from refreezer.infrastructure.stack import OutputKeys


if typing.TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
    from mypy_boto3_dynamodb import DynamoDBClient
    from mypy_boto3_s3 import S3Client
else:
    SFNClient = object
    DynamoDBClient = object
    S3Client = object


workflow_run_id = "workflow_run_123"


@pytest.fixture
def default_input() -> str:
    return json.dumps({"workflow_run": workflow_run_id})


@pytest.fixture(autouse=True, scope="module")
def setup() -> typing.Any:
    client: S3Client = boto3.client("s3")

    num_inventory_files = 2
    num_archives = 5
    archives_size_in_mb = 5
    file_name_prefix = f"{workflow_run_id}/test_inventory"

    for n in range(num_inventory_files):
        csv_buffer = generate_inventory_file(num_archives, archives_size_in_mb)

        client.put_object(
            Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
            Key=f"{file_name_prefix}_{n}.csv",
            Body=csv_buffer.getvalue().encode("utf-8"),
        )

    yield

    inventories_keys = [f"{file_name_prefix}_{n}.csv" for n in range(5)]
    client.delete_objects(
        Bucket=os.environ[OutputKeys.INVENTORY_BUCKET_NAME],
        Delete={"Objects": [{"Key": key} for key in inventories_keys]},
    )


def generate_inventory_file(num_archives: int, archives_size_in_mb: int) -> io.StringIO:
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 1, 1)

    archives_list = [
        [
            "".join(random.choices(string.ascii_letters + string.digits, k=138)),
            "Archive Description "
            + "".join(random.choices(string.ascii_letters + string.digits, k=130)),
            (
                start_date
                + timedelta(
                    seconds=random.randint(
                        0, int((end_date - start_date).total_seconds())
                    )
                )
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            archives_size_in_mb,
            binascii.hexlify(
                hashlib.sha256(os.urandom(2**20 * archives_size_in_mb)).digest()
            ).decode("utf-8"),
        ]
        for _ in range(num_archives)
    ]

    csv_buffer = io.StringIO()
    writer = csv.writer(
        csv_buffer,
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
        escapechar="\\",
        doublequote=False,
    )
    writer.writerow(
        [
            "ArchiveId",
            "ArchiveDescription",
            "CreationDate",
            "Size",
            "SHA256TreeHash",
        ]
    )
    writer.writerows(archives_list)

    return csv_buffer


def test_state_machine_start_execution(default_input: str) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None

    sf_history_output = client.get_execution_history(
        executionArn=response["executionArn"], maxResults=1000
    )

    event_details = [
        event["taskSucceededEventDetails"]
        for event in sf_history_output["events"]
        if "taskSucceededEventDetails" in event
    ]

    for detail in event_details:
        if detail["resourceType"] == "aws-sdk:dynamodb":
            state_output = json.loads(detail["output"])
            archive_id = state_output["job_result"]["ArchiveId"]

            table_name = os.environ[OutputKeys.GLACIER_RETRIEVAL_TABLE_NAME]
            db_client: DynamoDBClient = boto3.client("dynamodb")
            key = {"pk": {"S": f"IR:{archive_id}"}, "sk": {"S": "meta"}}
            item = db_client.get_item(TableName=table_name, Key=key)["Item"]
            assert item["job_id"] is not None and item["start_timestamp"] is not None
            break


def test_state_machine_nested_distributed_map(default_input: str) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INITIATE_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )

    sfn_util.wait_till_state_machine_finish(response["executionArn"], timeout=60)

    sf_history_output = client.get_execution_history(
        executionArn=response["executionArn"], maxResults=1000
    )

    events = [
        event
        for event in sf_history_output["events"]
        if "MapRunSucceeded" in event["type"]
    ]

    if not events:
        raise AssertionError(
            "Initiate retrieval nested distributed map failed to run successfully."
        )
