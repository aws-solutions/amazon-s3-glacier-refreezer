"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pathlib

header_lines = [
    '"""',
    "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.",
    "SPDX-License-Identifier: Apache-2.0",
    '"""',
]


def test_headers_exist() -> None:
    for path in pathlib.Path("source").glob("**/*.py"):
        if path.stat().st_size > 0:
            with open(path) as f:
                print(path)
                for line in header_lines:
                    assert line == f.readline().strip()
