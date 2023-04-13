"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import typing
import boto3
import time

if typing.TYPE_CHECKING:
    from mypy_boto3_stepfunctions import SFNClient
else:
    SFNClient = object


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
