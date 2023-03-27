"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest
from refreezer.application.model.glacier_transfer_model import (
    GlacierTransferModel,
)


def test_get_partition_key_name() -> None:
    job_item = GlacierTransferModel("run_id_1", "glacier_id_1", 1)
    assert job_item.partion_key_name == "pk"
    assert job_item.sort_key_name == "sk"
    assert job_item.sort_key_meta == "meta"
    assert job_item.partition_key == "run_id_1:glacier_id_1"
    assert job_item.sort_key_part == "p:0000001"
