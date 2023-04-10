"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest

from refreezer.application.chunking.trim import trim_inventory_chunk


@pytest.mark.parametrize(
    "chunk, max_record_size, first_chunk, last_chunk, expected",
    [
        (
            b"abcdef\nghi\nminopqrnq\nstqauvwx\nyz",
            10,
            True,
            False,
            b"abcdef\nghi\nminopqrnq\nstqauvwx",
        ),
        (
            b"abcdef\nghi\nstqauvwxa\nabcde\n",
            10,
            True,
            False,
            b"abcdef\nghi\nstqauvwxa\nabcde",
        ),
        (
            b"abcdef\nghi\nstqauvwxa\nabcdefghi\n",
            10,
            True,
            False,
            b"abcdef\nghi\nstqauvwxa\nabcdefghi",
        ),
        (
            b"abcdef\nghi\nstqauvwxa\nabcdefgh\n",
            10,
            True,
            False,
            b"abcdef\nghi\nstqauvwxa\nabcdefgh",
        ),
        (
            b"abcdef\nghi\nminopqrnq\nstqauvwx\nyz",
            10,
            False,
            True,
            b"\nghi\nminopqrnq\nstqauvwx\nyz",
        ),
        (
            b"\nabcdefghij\nghi\nstqauvwxa\nabcde",
            10,
            False,
            True,
            b"\nabcdefghij\nghi\nstqauvwxa\nabcde",
        ),
        (
            b"\nabcdefghi\nghi\nstqauvwxa\nabcde",
            10,
            False,
            True,
            b"\nabcdefghi\nghi\nstqauvwxa\nabcde",
        ),
        (
            b"\nabcdefgh\nghi\nstqauvwxa\nabcde",
            10,
            False,
            True,
            b"\nghi\nstqauvwxa\nabcde",
        ),
        (
            b"abcdefghi\nabcdefghi\nabcdefghi\nabcdefghi\nabcdefghi\n",
            10,
            False,
            True,
            b"\nabcdefghi\nabcdefghi\nabcdefghi\nabcdefghi\n",
        ),
        (
            b"abcdef\nghi\nminopqrnq\nstqauvwx\nyz",
            10,
            False,
            False,
            b"\nghi\nminopqrnq\nstqauvwx",
        ),
        (
            (
                b'RxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b"rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u"
            ),
            2**8,
            False,
            False,
            (
                b"\n"
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb'
            ),
        ),
    ],
)
def test_trim_inventory(
    chunk: bytes,
    max_record_size: int,
    first_chunk: bool,
    last_chunk: bool,
    expected: bytes,
) -> None:
    assert expected == trim_inventory_chunk(
        first_chunk, last_chunk, max_record_size, chunk
    )
