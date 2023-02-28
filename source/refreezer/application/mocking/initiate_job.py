"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from typing import List, Dict, Any
import random
import string
import random
from datetime import datetime, timedelta
import csv
import os
import hashlib
import boto3
from io import StringIO
import base64

client = boto3.client("s3")
mock_glacier_vault_bucket_name = os.environ["mock_glacier_vault_bucket_name"]
vault_name = os.environ["vault_name"]
max_file_size_in_mb = os.environ["max_file_size_in_mb"]
num_archives = os.environ["num_archives"]


def create_fake_archives() -> List[Dict[str, Any]]:
    # "ArchiveId": "ETyVX_NkWEBLq6oC8aT59BK2LywoJdIdf0FMwms-cx8nZxccnC1kTFIY7G2OEjqvWCQjVup076AUVy8hyek--F_mvQ-gKhuIoopCHdOeWdSU2Ytc-Z2tl_U5HtC1JSw3p4wYrLNIXw",
    # "ArchiveDescription": "test_1g_4",
    # "CreationDate": "2022-11-10T22:40:49Z",
    # "Size": 1073741824,
    # "SHA256TreeHash": "d60cc3cba62a74e2ffcd9874b1291bfcb654a21601c9ad101d77126455e12bb4"

    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 1, 1)
    archives_list = []

    for a in range(int(num_archives)):
        random_size = (
            (2**20) * random.randint(1, int(max_file_size_in_mb))
        ) + random.randint(0, (2**20))
        random_data = os.urandom(random_size)
        filename = f"archive_file_{a}.bin"
        client.put_object(
            Body=random_data,
            Bucket=mock_glacier_vault_bucket_name,
            Key=f"{vault_name}/archives/{filename}",
        )

        archive_id = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=138)
        )
        archive_description = "Archive Description " + "".join(
            random.choices(string.ascii_letters + string.digits, k=130)
        )
        random_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        creation_date = random_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        SHA256_tree_hash = generateTreeHash(random_data)

        archives_list.append(
            {
                "ArchiveId": str(archive_id),
                "ArchiveDescription": archive_description,
                "CreationDate": creation_date,
                "Size": random_size,
                "SHA256TreeHash": SHA256_tree_hash,
            }
        )
    return archives_list


def generateTreeHash(data: bytes) -> str:
    try:
        chunk_size = 1 * (2**20)
        hashes = []
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            hashes.append(hashlib.sha256(chunk).digest())
        return base64.b64encode(tree_hash(hashes)[0]).decode("utf-8")
    except Exception as e:
        raise e


def tree_hash(tree_hashes: List[bytes]) -> List[bytes]:
    while len(tree_hashes) > 1:
        next_hashes = []
        while len(tree_hashes) > 1:
            next_hashes.append(
                hashlib.sha256(tree_hashes.pop(0) + tree_hashes.pop(0)).digest()
            )
        if len(tree_hashes) > 0:
            next_hashes.append(tree_hashes.pop(0))
        tree_hashes = next_hashes
    return tree_hashes


def create_fake_inventory_file() -> None:
    archives_list = create_fake_archives()
    csv_string = StringIO()
    fieldnames = [
        "ArchiveId",
        "ArchiveDescription",
        "CreationDate",
        "Size",
        "SHA256TreeHash",
    ]
    writer = csv.DictWriter(csv_string, fieldnames=fieldnames)
    writer.writeheader()
    for archive in archives_list:
        writer.writerow(archive)

    client.put_object(
        Body=csv_string.getvalue(),
        Bucket=mock_glacier_vault_bucket_name,
        Key=f"{vault_name}/inventory.csv",
    )


def generate_inventory_retrieval_response() -> Dict[str, Any]:
    create_fake_inventory_file()
    sts_client = boto3.client("sts")
    account_id = sts_client.get_caller_identity().get("Account")
    job_id = "".join(random.choices(string.ascii_letters + string.digits, k=92))
    return {
        "location": f"/{account_id}/vaults/{vault_name}/jobs/{job_id}",
        "jobId": job_id,
    }
