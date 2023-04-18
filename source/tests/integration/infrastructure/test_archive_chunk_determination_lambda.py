"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import TYPE_CHECKING


from refreezer.infrastructure.stack import OutputKeys

if TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = object

# TODO: Add tests for the lambda functions
