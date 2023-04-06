"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


class ChunkSizeTooSmall(Exception):
    def __init__(self, chunk_size: int, maximum_inventory_record_size: int) -> None:
        self.message = f"Chunk size: {chunk_size} can not be smaller than maximum inventory record size: {maximum_inventory_record_size}"
        super().__init__(self.message)


class GlacierChecksumMismatch(Exception):
    def __init__(self) -> None:
        self.message = (
            "Calculated checksum did not match the checksum provided by Glacier."
        )
        super().__init__(self.message)


class AccessViolation(Exception):
    def __init__(self) -> None:
        self.message = "Resource was accessed innapropriately."
        super().__init__(self.message)


class ResourceNotFound(Exception):
    def __init__(self, id: str) -> None:
        self.message = f"Resource with id: {id} could not be found."
        super().__init__(self.message)


class StepFunctionFailure(Exception):
    def __init__(self, status: str) -> None:
        self.message = f"StepFunction failed with status: {status}"
        super().__init__(self.message)
