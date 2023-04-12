"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""


class OutputKeys:
    ARCHIVE_CHUNK_DETERMINATION_LAMBDA_ARN = "ArchiveChunkDeterminationLambdaArn"
    ASYNC_FACILITATOR_TABLE_NAME = "AsyncFacilitatorTableName"
    ASYNC_FACILITATOR_TOPIC_ARN = "AsyncFacilitatorTopicArn"
    OUTPUT_BUCKET_NAME = "OutputBucketName"
    INVENTORY_BUCKET_NAME = "InventoryBucketName"
    CHUNK_RETRIEVAL_LAMBDA_ARN = "ChunkRetrievalLambdaArn"
    CHUNK_VALIDATION_LAMBDA_ARN = "ChunkValidationLambdaArn"
    INVENTORY_CHUNK_RETRIEVAL_LAMBDA_ARN = "InventoryChunkRetrievalLambdaArn"
    INVENTORY_RETRIEVAL_STATE_MACHINE_ARN = "InventoryRetrievalStateMachineArn"
    INVENTORY_CHUNK_DETERMINATION_LAMBDA_ARN = "InventoryChunkDeterminationLambdaArn"
    ASYNC_FACILITATOR_LAMBDA_NAME = "AsyncFacilitatorLambdaName"
    INITIATE_RETRIEVAL_STATE_MACHINE_ARN = "InitiateRetrievalStateMachineArn"
    RETRIEVE_ARCHIVE_STATE_MACHINE_ARN = "RetrieveArchiveStateMachineArn"
    GLACIER_RETRIEVAL_TABLE_NAME = "GlacierRetrievalTableName"
    INVENTORY_VALIDATE_MULTIPART_UPLOAD_LAMBDA_ARN = (
        "InventoryValidateMultipartUploadLambdaArn"
    )
