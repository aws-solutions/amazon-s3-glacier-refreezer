"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import typing

import jsii
from aws_cdk import Stack, Stage, Aws
from aws_cdk import aws_codebuild as codebuild
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions
from aws_cdk import pipelines
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_iam as iam
from constructs import Construct

from refreezer.infrastructure.stack import RefreezerStack


class PipelineStack(Stack):
    """
    This stack establishes a pipeline that builds, deploys, and tests the solution
    in a specified account. It also uses CodeStar connections to set up a webhook
    to GitHub to trigger the pipeline when commits are pushed.

    The repo is configured using SSM parameters, specifically the following:

       - /refreezer-build/connection/owner
          - GitHub repo owner
       - /refreezer-build/connection/repo
          - GitHub repo name
       - /refreezer-build/connection/branch
          - GitHub repo branch
       - /refreezer-build/connection/arn
          - CodeStar Connection ARN

    Set up the connection by following the documentation at
    https://docs.aws.amazon.com/dtconsole/latest/userguide/connections-create-github.html
    """

    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        connection = CodeStarSource(
            name="CodeStarConnection",
            connection_arn=ssm.StringParameter.value_for_string_parameter(
                self, "/refreezer-build/connection/arn"
            ),
            owner=ssm.StringParameter.value_for_string_parameter(
                self, "/refreezer-build/connection/owner"
            ),
            repo=ssm.StringParameter.value_for_string_parameter(
                self, "/refreezer-build/connection/repo"
            ),
            branch=ssm.StringParameter.value_for_string_parameter(
                self, "/refreezer-build/connection/branch"
            ),
        )

        pipeline = pipelines.CodePipeline(
            self,
            "Pipeline",
            synth=pipelines.CodeBuildStep(
                "Synth",
                input=connection,
                install_commands=[
                    'pip install ".[dev]"',
                    "tox -- --junitxml=pytest-report.xml",
                ],
                commands=[
                    "npx cdk synth",
                ],
                partial_build_spec=codebuild.BuildSpec.from_object(
                    {
                        "reports": {
                            "pytest_reports": {
                                "files": ["pytest-report.xml"],
                                "file-format": "JUNITXML",
                            }
                        }
                    }
                ),
            ),
            code_build_defaults=pipelines.CodeBuildOptions(
                build_environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_6_0,
                    compute_type=codebuild.ComputeType.LARGE,
                )
            ),
        )

        pipeline.add_stage(DeployStage(self, "Deploy"))

        test_wave = pipeline.add_wave("Test")
        test_wave.add_post(
            pipelines.CodeBuildStep(
                "IntegrationTest",
                install_commands=[
                    "pip install tox",
                ],
                commands=[
                    "tox -e integration -- --junitxml=pytest-integration-report.xml"
                ],
                role_policy_statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["ssm:Describe*", "ssm:Get*", "ssm:List*"],
                        resources=[
                            f"arn:aws:ssm:{Aws.REGION}:{Aws.ACCOUNT_ID}:parameter/refreezer/*"
                        ],
                    )
                ],
                partial_build_spec=codebuild.BuildSpec.from_object(
                    {
                        "reports": {
                            "pytest_reports": {
                                "files": ["pytest-integration-report.xml"],
                                "file-format": "JUNITXML",
                            }
                        }
                    }
                ),
            )
        )


class DeployStage(Stage):
    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        RefreezerStack(self, "refreezer")


class CodeStarSource(pipelines.CodePipelineSource):
    """
    We need another class here instead of using the factory .connection() because
    the factory uses the owner/repo string as the name for the construct. Since we're
    looking up the connection configuration during deploy using SSM, we need to specify
    a static name for synthesis.

    The implementation here is the same as what is produced from the factory method, but
    with the addition of statically defining the name.
    """

    def __init__(
        self, name: str, owner: str, repo: str, branch: str, connection_arn: str
    ):
        super(CodeStarSource, self).__init__(name)
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self.connection_arn = connection_arn
        super(CodeStarSource, self)._configure_primary_output(
            pipelines.FileSet("Source", self)
        )

    def _get_action(
        self,
        output: codepipeline.Artifact,
        action_name: str,
        run_order: jsii.Number,
        variables_namespace: typing.Optional[str] = None,
    ) -> codepipeline_actions.Action:
        return codepipeline_actions.CodeStarConnectionsSourceAction(
            connection_arn=self.connection_arn,
            output=output,
            owner=self.owner,
            repo=self.repo,
            branch=self.branch,
            action_name=action_name,
            run_order=run_order,
            variables_namespace=variables_namespace,
        )
