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
else:
    SFNClient = object


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
    sf_output = get_state_machine_output(response["executionArn"], timeout=20)
    assert "InventoryRetrieved" in sf_output


def test_initiate_job_task_succeeded(default_input: str) -> None:
    client: SFNClient = boto3.client("stepfunctions")
    response = client.start_execution(
        stateMachineArn=os.environ[OutputKeys.INVENTORY_RETRIEVAL_STATE_MACHINE_ARN],
        input=default_input,
    )

    wait_till_state_machine_finish(response["executionArn"], timeout=20)

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
