"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import logging

logger = logging.getLogger()


class GlacierTransferModel:
    def __init__(self, run_id: str, glacier_object_id: str, part_number: int) -> None:
        self.run_id = run_id
        self.partion_key_name = "pk"
        self.sort_key_name = "sk"
        self.sort_key_meta = "meta"
        self.glacier_object_id = glacier_object_id
        self.part_number = part_number
        self.partition_key = run_id + ":" + glacier_object_id
        self.sort_key_part = f"p:{part_number:07d}"
