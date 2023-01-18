"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import (
    Stack,
)
from aws_cdk import aws_ssm as ssm
from constructs import Construct


class RefreezerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        ssm.StringParameter(
            self,
            "SomeParameter",
            parameter_name="/refreezer/parameter",
            string_value="foo",
        )
