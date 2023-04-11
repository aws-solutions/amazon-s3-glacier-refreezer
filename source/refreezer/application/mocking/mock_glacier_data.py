"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""
MOCK_DATA = {
    "vault1": {
        "initiate-job": {
            "inventory-retrieval": {
                "ResponseMetadata": {
                    "HTTPStatusCode": 202,
                    "HTTPHeaders": {},
                    "RetryAttempts": 0,
                },
                "location": "//vaults/vault1/jobs/XYRPIPBXI8YIFXQDR82UXKHDT03L8JY03398U1I5EMVCGIL9AYUAD9AZN2N582OGQPGG9XD89A2N245SW3N443RNV8H8",
                "jobId": "XYRPIPBXI8YIFXQDR82UXKHDT03L8JY03398U1I5EMVCGIL9AYUAD9AZN2N582OGQPGG9XD89A2N245SW3N443RNV8H8",
            },
            "archive-retrieval:n098f6bcd4621d373cade4e832627b4f6": {
                "ResponseMetadata": {
                    "HTTPStatusCode": 202,
                    "HTTPHeaders": {},
                    "RetryAttempts": 0,
                },
                "location": "//vaults/vault1/jobs/test-job-id",
                "jobId": "test-job-id",
            },
        },
        "get-job-output": {
            "XYRPIPBXI8YIFXQDR82UXKHDT03L8JY03398U1I5EMVCGIL9AYUAD9AZN2N582OGQPGG9XD89A2N245SW3N443RNV8H8": {
                "ResponseMetadata": {
                    "HTTPStatusCode": 200,
                    "HTTPHeaders": {},
                    "RetryAttempts": 0,
                },
                "status": 200,
                "contentType": "application/json",
                "body": "ArchiveId,ArchiveDescription,CreationDate,Size,SHA256TreeHash\n098f6bcd4621d373cade4e832627b4f6,,2023-04-11T15:18:41.000Z,4,9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
            },
            "test-job-id": {
                "0-1023": {
                    "ResponseMetadata": {
                        "HTTPStatusCode": 200,
                        "HTTPHeaders": {},
                        "RetryAttempts": 0,
                    },
                    "status": 200,
                    "contentType": "application/json",
                    "body": "test body",
                    "checksum": "some-checksum",
                }
            },
        },
    }
}
