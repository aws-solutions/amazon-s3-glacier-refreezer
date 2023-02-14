"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import Stack, Stage, Aws, CfnOutput
from aws_cdk import aws_ssm as ssm
from aws_cdk import RemovalPolicy
from constructs import Construct


class BootstrapStack(Stack):
    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        param_repo_owner = ssm.StringParameter(
            self,
            "repo_owner",
            description="GitHub repo owner",
            parameter_name="/refreezer-build/connection/owner",
            string_value="Initial parameter value",
        )

        param_repo_name = ssm.StringParameter(
            self,
            "repo_name",
            description="GitHub repo name",
            parameter_name="/refreezer-build/connection/repo",
            string_value="Initial parameter value",
        )

        param_repo_branch = ssm.StringParameter(
            self,
            "repo_branch",
            description="GitHub repo branch",
            parameter_name="/refreezer-build/connection/branch",
            string_value="Initial parameter value",
        )

        param_codestar_connection = ssm.StringParameter(
            self,
            "code_arn",
            description="CodeStar Connection ARN",
            parameter_name="/refreezer-build/connection/arn",
            string_value="Initial parameter value",
        )
