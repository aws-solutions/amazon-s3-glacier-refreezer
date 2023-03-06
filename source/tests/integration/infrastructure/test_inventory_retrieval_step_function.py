"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import boto3
import time
import json
from refreezer.infrastructure.stack import OutputKeys
import pytest

if typing.TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
    from mypy_boto3_dynamodb import DynamoDBClient
else:
    SFNClient = object
    DynamoDBClient = object


@pytest.fixture
def default_input() -> str:
    topic_arn = os.environ[OutputKeys.ASYNC_FACILITATOR_TOPIC_ARN]
    return json.dumps(
        dict(
            provided_inventory="NO",
            vault_name="test-vault-01",
            description="This is a test",
            sns_topic=topic_arn,
        )
    )


def test_state_machine_start_execution() -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN]
    )
    assert 200 == response["ResponseMetadata"]["HTTPStatusCode"]
    assert response["executionArn"] is not None


def test_state_machine_start_execution_provided_inventory_yes() -> None:
    client: SFNClient = boto3.client("stepfunctions")
    input = '{"provided_inventory": "YES"}'
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input=input,
    )
    sf_output = get_state_machine_output(response["executionArn"], timeout=10)
    assert "retrieveInventory" not in sf_output


def test_state_machine_start_execution_provided_inventory_no(
    default_input: str,
) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )
    sf_output = get_state_machine_output(response["executionArn"], timeout=60)
    assert "InventoryRetrieved" in sf_output


def test_initiate_job_task_succeeded(default_input: str) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )

    wait_till_state_machine_finish(response["executionArn"], timeout=60)

    sf_history_output = client.get_execution_history(
        executionArn=response["executionArn"], maxResults=1000
    )
    event_details = [
        event["stateExitedEventDetails"]
        for event in sf_history_output["events"]
        if "stateExitedEventDetails" in event
    ]

    for detail in event_details:
        if detail["name"] == "MockGlacierInitiateJobTask":
            state_output = detail["output"]
            assert "JobId" in state_output and "Location" in state_output
            break


def test_dynamo_db_put_item_async_behavior(default_input: str) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )

    wait_till_state_machine_finish(response["executionArn"], timeout=60)

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
            job_id = state_output["job_result"]["JobId"]

            table_name = os.environ[OutputKeys.ASYNC_FACILITATOR_TABLE_NAME]
            db_client: DynamoDBClient = boto3.client("dynamodb")
            key = {"job_id": {"S": job_id}}
            item = db_client.get_item(TableName=table_name, Key=key)["Item"]
            assert (
                item["task_token"] is not None and item["finish_timestamp"] is not None
            )
            break


def get_state_machine_output(executionArn: str, timeout: int) -> str:
    client: SFNClient = boto3.client("stepfunctions")
    start_time = time.time()
    sf_output: str = "TIMEOUT EXCEEDED"
    while (time.time() - start_time) < timeout:
        time.sleep(1)
        sf_describe_response = client.describe_execution(executionArn=executionArn)
        status = sf_describe_response["status"]
        if status == "RUNNING":
            continue
        elif status == "SUCCEEDED":
            sf_output = sf_describe_response["output"]
            break
        else:
            # for status: FAILED, TIMED_OUT or ABORTED
            raise Exception(f"Execution {status}")

    return sf_output


def wait_till_state_machine_finish(executionArn: str, timeout: int) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    start_time = time.time()
    while (time.time() - start_time) < timeout:
        time.sleep(1)
        sf_describe_response = client.describe_execution(executionArn=executionArn)
        status = sf_describe_response["status"]
        if status == "RUNNING":
            continue
        break
