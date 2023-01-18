"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import typing

import boto3

if typing.TYPE_CHECKING:
    from mypy_boto3_ssm import SSMClient
else:
    SSMClient = object


def test_parameter_has_correct_value() -> None:
    client: SSMClient = boto3.client("ssm")
    assert (
        "foo" == client.get_parameter(Name="/refreezer/parameter")["Parameter"]["Value"]
    )
