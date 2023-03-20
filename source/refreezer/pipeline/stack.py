"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import Mapping

from aws_cdk import Stack, Stage, Aws, CfnOutput
from aws_cdk import aws_codebuild as codebuild
from aws_cdk import pipelines
from aws_cdk import aws_ssm as ssm
from aws_cdk import aws_iam as iam
from constructs import Construct

from refreezer.pipeline.source import CodeStarSource
from refreezer.infrastructure.stack import RefreezerStack
from refreezer.mocking.mock_glacier_stack import MockGlacierStack

DEPLOY_STAGE_NAME = "test-deploy"
REFREEZER_STACK_NAME = "refreezer"
MOCK_GLACIER_STACK_NAME = "mock-glacier"
STACK_NAME = f"{DEPLOY_STAGE_NAME}-{REFREEZER_STACK_NAME}"


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

        pipeline = pipelines.CodePipeline(
            self,
            "Pipeline",
            synth=self.get_synth_step(),
            code_build_defaults=pipelines.CodeBuildOptions(
                build_environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_6_0,
                    compute_type=codebuild.ComputeType.LARGE,
                )
            ),
        )

        deploy_stage = DeployStage(self, DEPLOY_STAGE_NAME)
        pipeline.add_stage(
            deploy_stage,
            post=[
                self.get_integration_test_step(
                    outputs_map=deploy_stage.refreezer_stack.outputs
                )
            ],
        )

    def get_connection(self) -> CodeStarSource:
        return CodeStarSource(
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

    def get_synth_step(self) -> pipelines.CodeBuildStep:
        return pipelines.CodeBuildStep(
            "Synth",
            input=self.get_connection(),
            install_commands=[
                'pip install ".[dev]"',
                "tox -- --junitxml=pytest-report.xml",
            ],
            commands=[
                "npx cdk synth",
            ],
            partial_build_spec=self.get_reports_partial_build_spec("pytest-report.xml"),
        )

    def get_integration_test_step(
        self, outputs_map: Mapping[str, CfnOutput]
    ) -> pipelines.CodeBuildStep:
        return pipelines.CodeBuildStep(
            "IntegrationTest",
            install_commands=[
                "pip install tox",
            ],
            commands=["tox -e integration -- --junitxml=pytest-integration-report.xml"],
            env_from_cfn_outputs=outputs_map,
            role_policy_statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:DeleteItem",
                    ],
                    resources=[
                        (
                            f"arn:aws:dynamodb:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/"
                            f"{STACK_NAME}-AsyncFacilitatorTable*"
                        )
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["sns:Publish", "sns:ListSubscriptionsByTopic"],
                    resources=[
                        f"arn:aws:sns:{Aws.REGION}:{Aws.ACCOUNT_ID}:{STACK_NAME}-AsyncFacilitatorTopic*"
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:PutObject", "s3:DeleteObject", "s3:GetObject"],
                    resources=[
                        f"arn:aws:s3:::{STACK_NAME}-outputbucket*",
                        f"arn:aws:s3:::{STACK_NAME}-inventorybucket*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:StartExecution",
                        "states:DescribeExecution",
                        "states:GetExecutionHistory",
                    ],
                    resources=[
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:InventoryRetrievalStateMachine*",
                        f"arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:execution:InventoryRetrievalStateMachine*",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["lambda:InvokeFunction"],
                    resources=[
                        f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:{STACK_NAME}-ChunkRetrieval*"
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["lambda:InvokeFunction", "lambda:GetFunction"],
                    resources=[
                        f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:{STACK_NAME}-AsyncFacilitator*"
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["lambda:InvokeFunction"],
                    resources=[
                        f"arn:aws:lambda:{Aws.REGION}:{Aws.ACCOUNT_ID}:function:{STACK_NAME}-InventoryChunkDetermination*"
                    ],
                ),
            ],
            partial_build_spec=self.get_reports_partial_build_spec(
                "pytest-integration-report.xml"
            ),
        )

    def get_reports_partial_build_spec(self, filename: str) -> codebuild.BuildSpec:
        return codebuild.BuildSpec.from_object(
            {
                "reports": {
                    "pytest_reports": {
                        "files": [filename],
                        "file-format": "JUNITXML",
                    }
                }
            }
        )


class DeployStage(Stage):
    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)
        mock_glacier_stack = MockGlacierStack(self, MOCK_GLACIER_STACK_NAME)
        self.refreezer_stack = RefreezerStack(
            self, REFREEZER_STACK_NAME, mock_glacier_stack.params
        )
