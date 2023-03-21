"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import pytest

from refreezer.application.chunking.inventory import generate_chunk_array
from refreezer.application.chunking.trim import trim_inventory_chunk


@pytest.mark.parametrize(
    "data, max_record_size, chunk_size",
    [
        (
            (
                b"abcdefghi\nghi\nminopqrnq\nstqauvwx\nabcdefghi\nabcdefghi\nghi\nminopqrnq\nabcdefghi\nasd\nabcr"
                b"sdfhr\nqwertyuikj\nqwertyui\nstqauvwx\nabcde\nabcdefghi\nghi\nminopqrnq\nstqauvwx\nabcdefghi\n"
                b"abcdefghi\nghi\nminopqrnq\nstqauvwx\nabcde"
            ),
            10,
            30,
        ),
        (
            (
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'ETyVX_NkWEBLq6oC8aT59BK2LywoJdIdf0FMwms-cx8nZxccnC1kTFIY7G2OEjqvWCQjVup076AUVy8hyek--F_mvQ-gKhuIoopCHdOeWdSU2Ytc-Z2tl_U5HtC1JSw3p4wYrLNIXw,"test_1g_4",2022-11-10T22:40:49Z,1073741824,d60cc3cba62a74e2ffcd9874b1291bfcb654a21601c9ad101d77126455e12bb4\n'
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'ETyVX_NkWEBLq6oC8aT59BK2LywoJdIdf0FMwms-cx8nZxccnC1kTFIY7G2OEjqvWCQjVup076AUVy8hyek--F_mvQ-gKhuIoopCHdOeWdSU2Ytc-Z2tl_U5HtC1JSw3p4wYrLNIXw,"test_1g_4",2022-11-10T22:40:49Z,1073741824,d60cc3cba62a74e2ffcd9874b1291bfcb654a21601c9ad101d77126455e12bb4\n'
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'ETyVX_NkWEBLq6oC8aT59BK2LywoJdIdf0FMwms-cx8nZxccnC1kTFIY7G2OEjqvWCQjVup076AUVy8hyek--F_mvQ-gKhuIoopCHdOeWdSU2Ytc-Z2tl_U5HtC1JSw3p4wYrLNIXw,"test_1g_4",2022-11-10T22:40:49Z,1073741824,d60cc3cba62a74e2ffcd9874b1291bfcb654a21601c9ad101d77126455e12bb4\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
                b'ETyVX_NkWEBLq6oC8aT59BK2LywoJdIdf0FMwms-cx8nZxccnC1kTFIY7G2OEjqvWCQjVup076AUVy8hyek--F_mvQ-gKhuIoopCHdOeWdSU2Ytc-Z2tl_U5HtC1JSw3p4wYrLNIXw,"test_1g_4",2022-11-10T22:40:49Z,1073741824,d60cc3cba62a74e2ffcd9874b1291bfcb654a21601c9ad101d77126455e12bb4\n'
                b'rSWlxgkNslVWE8YWd_T-qUjtmVLImx4AVe07GpW1JrfqlRSLhDUyrMlM0Wx86gUagOwAv2ynH5qe56YwQ_mxQh6C8zWKyRc_4EuXbPWwAza6_odied0VGBkfkCBi5u5SSWcB5R2Uyw,"",2023-03-20T00:16:53Z,524288000,a38b0bd75ea26b607b6215d9cbe8b67772f7a5ef8faa7907c4cf8a4152875e90\n'
                b'zfThRt6rNrhhhv4rVi2TARKqWrQBOJBgdrZ_Mlr76GEahnjgSUYiXfOMhCgE76VRPMpieipbCWdDxniYP5ebRmueTomDHEbI4iRxM10Zfuw2h_gBEbTs6flwr77-r86cti5Aa1x3Zg,"test-vault-01 multipart upload test",2023-02-23T17:06:45Z,3145728,bfa0c616ded790818712ceef0202c56e49d7177aea72442cbf3bf5a6ff355ceb\n'
            ),
            2**9,
            2**9 * 2,
        ),
    ],
)
def test_chunk_trim_reassemble(
    data: bytes, max_record_size: int, chunk_size: int
) -> None:
    chunks_range = generate_chunk_array(len(data), max_record_size, chunk_size)
    result = b""
    for chunk_range in chunks_range:
        start, end = map(int, chunk_range.split("-"))
        chunk = data[start : end + 1]

        first_chunk = start == 0
        last_chunk = end == len(data) - 1
        result += trim_inventory_chunk(first_chunk, last_chunk, max_record_size, chunk)

    assert data == result
