"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import typing
import boto3
import json

from refreezer.infrastructure.stack import OutputKeys

if typing.TYPE_CHECKING:
    from mypy_boto3_lambda import LambdaClient
else:
    LambdaClient = object

# TODO: Add tests for the lambda functions
