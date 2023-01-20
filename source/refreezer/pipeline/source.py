"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import jsii
import typing
from aws_cdk import pipelines
from aws_cdk import aws_codepipeline as codepipeline
from aws_cdk import aws_codepipeline_actions as codepipeline_actions


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
