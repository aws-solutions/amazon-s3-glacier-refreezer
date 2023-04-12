"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

from aws_cdk import CfnOutput
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_stepfunctions as sfn
from cdk_nag import NagSuppressions
from constructs import Construct
from typing import Optional

from refreezer.mocking.mock_glacier_stack import MockingParams
from refreezer.infrastructure.output_keys import OutputKeys


class ArchiveRetrievalWorkflow:
    def __init__(
        self,
        scope: Construct,
        async_facilitator_table: dynamodb.Table,
        glacier_retrieval_table: dynamodb.Table,
        inventory_bucket: s3.Bucket,
        output_bucket: s3.Bucket,
        outputs: dict[str, CfnOutput],
        mock_params: Optional[MockingParams] = None,
    ) -> None:
        # TODO: To be replaced by DynamoDB GetItem (Synchronous mode)
        retrieve_archive_dynamo_db_get_job = sfn.Pass(
            scope, "RetrieveArchiveDynamoDBGetJob"
        )

        # TODO: To be replaced by DynamoDB Put custom state for Step Function SDK integration
        # pause the workflow using waitForTaskToken mechanism
        retrieve_archive_dynamo_db_put = sfn.Pass(scope, "RetrieveArchiveDynamoDBPut")

        # TODO: To be replaced by s3:createMultipartUpload task
        retrieve_archive_start_multipart_upload = sfn.Pass(
            scope, "RetrieveArchiveStartMultipartUpload"
        )

        # TODO: To be replaced by generate chunk array LambdaInvoke task
        retrieve_archive_generate_chunk_array_lambda_task = sfn.Pass(
            scope, "RetrieveArchiveGenerateChunkArrayLambda"
        )

        # TODO: To be replaced by chunk processing LambdaInvoke task
        retrieve_archive_chunk_processing_lambda_task = sfn.Pass(
            scope, "RetrieveArchivechunkProcessingLambdaTask"
        )

        # TODO: To be replaced by a Map state in Distributed mode
        retrieve_archive_chunk_distributed_map_state = sfn.Map(
            scope, "RetrieveArchiveChunkDistributedMap"
        )
        retrieve_archive_chunk_distributed_map_state.iterator(
            retrieve_archive_chunk_processing_lambda_task
        )

        retrieve_archive_definition = (
            retrieve_archive_dynamo_db_get_job.next(retrieve_archive_dynamo_db_put)
            .next(retrieve_archive_start_multipart_upload)
            .next(retrieve_archive_generate_chunk_array_lambda_task)
            .next(retrieve_archive_chunk_distributed_map_state)
        )

        # TODO: To be replaced by nested Map states in Distributed mode
        retrieve_archive_distributed_map_state = sfn.Map(
            scope, "RetrieveArchiveDistributedMap"
        )

        # TODO: To be replaced by nested Map states in Distributed mode
        retrieve_archive_inner_distributed_map_state = sfn.Map(
            scope, "RetrieveArchiveInnerDistributedMap"
        )
        retrieve_archive_inner_distributed_map_state.iterator(
            retrieve_archive_definition
        )

        retrieve_archive_distributed_map_state.iterator(
            retrieve_archive_inner_distributed_map_state
        )

        # TODO: To be replaced by validate LambdaInvoke task
        retrieve_archive_validate_lambda_task = sfn.Pass(
            scope, "RetrieveArchiveValidateLambdaTask"
        )

        # TODO: To be replaced by s3:abortMultipartUpload
        retrieve_archive_close_multipart_upload = sfn.Pass(
            scope, "RetrieveArchiveCloseMultipartUpload"
        )

        retrieve_archive_state_machine = sfn.StateMachine(
            scope,
            "RetrieveArchiveStateMachine",
            definition=retrieve_archive_distributed_map_state.next(
                retrieve_archive_validate_lambda_task.next(
                    retrieve_archive_close_multipart_upload
                )
            ),
        )

        outputs[OutputKeys.RETRIEVE_ARCHIVE_STATE_MACHINE_ARN] = CfnOutput(
            scope,
            OutputKeys.RETRIEVE_ARCHIVE_STATE_MACHINE_ARN,
            value=retrieve_archive_state_machine.state_machine_arn,
        )

        NagSuppressions.add_resource_suppressions(
            retrieve_archive_state_machine,
            [
                {
                    "id": "AwsSolutions-SF1",
                    "reason": "Step Function logging is disabled and will be addressed later.",
                },
                {
                    "id": "AwsSolutions-SF2",
                    "reason": "Step Function X-Ray tracing is disabled and will be addressed later.",
                },
            ],
        )
