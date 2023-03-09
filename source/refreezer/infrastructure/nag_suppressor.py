"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import List, Optional, Any
from cdk_nag import NagSuppressions

ID_REASON_MAP = {
    "AwsSolutions-S1": {
        "reason": "Inventory Bucket has server access logs disabled and will be addressed later."
    },
    "AwsSolutions-IAM4": {
        "reason": "CDK grants AWS managed policy for Lambda basic execution by default. Replacing it with a customer managed policy will be addressed later.",
        "applies_to": "Policy::arn:<AWS::Partition>:iam::aws:policy/{}",
    },
    "AwsSolutions-SF1": {
        "reason": "Step Function logging is disabled and will be addressed later."
    },
    "AwsSolutions-SF2": {
        "reason": "Step Function X-Ray tracing is disabled and will be addressed later."
    },
}


def nagSuppressor(
    nag_obj: Any,
    nag_id_list: List[str],
    applies_to: Optional[list[str]] = None,
    custom_reason: Optional[str] = None,
) -> None:
    suppressions = []
    for nag_id in nag_id_list:
        reason = custom_reason or ID_REASON_MAP[nag_id]["reason"]
        suppression = {"id": nag_id, "reason": reason}
        add_applies_to(suppression, nag_id, applies_to)
        suppressions.append(suppression)
    NagSuppressions.add_resource_suppressions(nag_obj, suppressions)


def add_applies_to(
    suppression: Any, nag_id: str, applies_to: Optional[list[str]]
) -> None:
    if ID_REASON_MAP[nag_id].get("applies_to") and applies_to:
        suppression["appliesTo"] = []
        for apply_to in applies_to:
            applies_to_str = ID_REASON_MAP[nag_id]["applies_to"].format(apply_to)
            suppression["appliesTo"].append(applies_to_str)
