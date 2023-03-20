"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
"""

import os
import json
import logging
from typing import List, Dict, TYPE_CHECKING, Optional, Union, Any

from refreezer.application.db_accessor.dynamoDb_accessor import DynamoDBAccessor
from refreezer.application.model.facilitator import (
    FacilitatorModel,
    StatusCode,
)

if TYPE_CHECKING:
    from mypy_boto3_stepfunctions.client import SFNClient
else:
    SFNClient = object

logger = logging.getLogger()


def _update_db_entry(facilitator_model: FacilitatorModel) -> None:
    ddb_accessor = DynamoDBAccessor(os.getenv("DDB_TABLE_NAME", ""))
    items: Optional[List[Dict[str, Union[Any, Any]]]] = ddb_accessor.query_items(
        facilitator_model.primary_key, facilitator_model.job_id
    )

    if not items:
        raise Exception(
            f"The job with id: {facilitator_model.job_id} is not in the database"
        )

    for key, val in items[0].items():
        setattr(facilitator_model, key, val)

    ddb_accessor.insert_item(facilitator_model.__dict__)


def sns_handler(
    record: Dict[str, Any],
    sfn_client: SFNClient,
) -> None:
    job_result = json.loads(record["Sns"]["Message"])
    job_id = job_result["JobId"]
    facilitator_model = FacilitatorModel(job_id=job_id)
    facilitator_model.job_result = job_result
    if facilitator_model.check_still_in_progress():
        return

    facilitator_model.finish_timestamp = job_result["CompletionDate"]
    _update_db_entry(facilitator_model)
    if not facilitator_model.task_token:
        logger.info(f"Cannot find the task token for job {job_id}")
        return

    if not facilitator_model.check_job_success_status():
        sfn_client.send_task_failure(
            taskToken=facilitator_model.task_token,
            error="{}".format(job_result["StatusMessage"]),
            cause="",
        )
    else:
        sfn_client.send_task_success(
            taskToken=facilitator_model.task_token,
            output=json.dumps(
                {
                    "job_result": job_result,
                    "timestamp": record["Sns"]["Timestamp"],
                }
            ),
        )


def dynamoDb_handler(record: Dict[str, Any], sfn_client: SFNClient) -> None:
    if (
        new_image := record.get("dynamodb", {}).get("NewImage")
    ) is None or "finish_timestamp" not in new_image:
        logger.info("The job is still in progress")
        return

    task_token: str = new_image["task_token"]["S"]
    job_id: str = new_image["job_id"]["S"]
    if new_image["job_result"]["M"]["StatusCode"]["S"] == StatusCode.SUCCEEDED:
        logger.info(
            f"The job with {job_id} has succeeded, Succesfully updated the database"
        )
        sfn_client.send_task_success(
            taskToken=task_token,
            output=json.dumps(
                {
                    "job_result": "Success",
                    "timestamp": new_image["finish_timestamp"],
                }
            ),
        )
    else:
        logger.error(f"FAILED:: The job {job_id} has failed status")
        sfn_client.send_task_failure(taskToken=task_token, error="", cause="")
