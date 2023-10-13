import logging
from datetime import datetime
from functools import cached_property
from typing import Dict, Optional

import asana
from asana.models.task_response_data import TaskResponseData
from asana.rest import ApiException
from status_utils import Statuses, get_hubmap_id_from_uuid, get_submission_context

from airflow.configuration import conf as airflow_conf

HUBMAP_ID_FIELD_GID = "1204584344373112"
PROCESS_STAGE_FIELD_GID = "1204584344373114"
PROCESS_STAGE_GIDS = {
    "Intake": "1204584344373115",
    "Pre-Processing": "1204584344373116",
    "Ready Backlog": "1204584347884579",
    "Processing": "1204584347884580",
    "Post-Processing": "1204584347884581",
    "Publishing": "1204584347884582",
    "Blocked 🛑": "1204584347884583",
    "Completed": "1204584347884584",
}
# TODO: parent and entity fields/enums do not yet exist in Asana
PARENT_FIELD_GID = ""
ENTITY_TYPE_FIELD_GID = ""
ENTITY_TYPE_GIDS = {}


class UpdateAsana:
    def __init__(
        self,
        uuid: Optional[str],
        token: str,
        status: Optional[Statuses],
    ):
        self.uuid = uuid
        self.token = token
        self.status = status
        self.workspace = airflow_conf.as_dict()["connections"]["ASANA_WORKSPACE"]
        self.project = airflow_conf.as_dict()["connections"]["ASANA_PROJECT"]
        asana_token = airflow_conf.as_dict()["connections"]["ASANA_TOKEN"]
        # airflow_conf.as_dict() potentially returns a tuple;
        # this mollifies type-checking and provides a sensible error.
        assert isinstance(self.workspace, str), "ASANA_WORKSPACE is not a string!"
        assert isinstance(self.project, str), "ASANA_PROJECT is not a string!"
        assert isinstance(asana_token, str), "ASANA_TOKEN is not a string!"

        configuration = asana.Configuration()
        configuration.access_token = asana_token
        self.client = asana.ApiClient(configuration)
        self.tasks_client = asana.TasksApi(self.client)

    @cached_property
    def asana_status_map(self):
        return {
            Statuses.DATASET_ERROR: PROCESS_STAGE_GIDS["Blocked 🛑"],
            Statuses.DATASET_INVALID: PROCESS_STAGE_GIDS["Blocked 🛑"],
            Statuses.DATASET_NEW: PROCESS_STAGE_GIDS["Intake"],
            Statuses.DATASET_PROCESSING: "",
            Statuses.DATASET_PUBLISHED: PROCESS_STAGE_GIDS["Publishing"],
            Statuses.DATASET_QA: PROCESS_STAGE_GIDS["Post-Processing"],
            Statuses.UPLOAD_ERROR: PROCESS_STAGE_GIDS["Blocked 🛑"],
            Statuses.UPLOAD_INVALID: PROCESS_STAGE_GIDS["Blocked 🛑"],
            Statuses.UPLOAD_NEW: PROCESS_STAGE_GIDS["Intake"],
            Statuses.UPLOAD_PROCESSING: "",
            Statuses.UPLOAD_REORGANIZED: PROCESS_STAGE_GIDS["Processing"],
            Statuses.UPLOAD_SUBMITTED: PROCESS_STAGE_GIDS["Pre-Processing"],
            Statuses.UPLOAD_VALID: PROCESS_STAGE_GIDS["Pre-Processing"],
        }

    @cached_property
    def hubmap_id(self):
        if self.uuid is not None:
            return get_hubmap_id_from_uuid(self.token, self.uuid)

    @cached_property
    def submission_data(self):
        if self.uuid is not None:
            return get_submission_context(self.token, self.uuid)
        else:
            raise Exception("Cannot fetch data from Entity API. No uuid passed.")

    @cached_property
    def get_task_by_hubmap_id(self) -> str:
        """
        Given a HuBMAP ID, find the associated Asana task.
        Fails if a HuBMAP ID is associated with 0 or >1 tasks.
        """
        custom_field = f"custom_fields_{HUBMAP_ID_FIELD_GID}_contains"
        response_list = self.tasks_client.search_tasks_for_workspace(
            self.workspace, **{custom_field: self.hubmap_id}
        )
        response_length = len(response_list)
        if response_length == 1:
            task_id = response_list[0]["gid"]
        # TODO: this is not so great
        elif response_length > 1:
            types = []
            gids = []
            for response in response_list:
                for field in response["custom_fields"]:
                    if (
                        field["name"] == "Entity Type"
                        and (entity_type := field["enum_value"]["name"]) == "Dataset"
                    ):
                        types.append(entity_type)
                        gids.append(response["gid"])
            if len(types) > 1:
                task_gids = [task["gid"] for task in response_list]
                raise Exception(
                    f"""Multiple tasks with the entity_type tag 'Dataset' found
                    for HuBMAP ID {self.hubmap_id}. GIDs found: {task_gids}"""
                )
            elif len(types) == 0:
                raise Exception(
                    f"No tasks with type 'Dataset' found for HuBMAP ID {self.hubmap_id}."
                )
            task_id = gids[0]
        else:
            logging.info(
                f"""
                Error retrieving task by HuBMAP ID for {self.hubmap_id}!
                {response_length} results found.
                Check that a task for the expected HuBMAP ID exists in Asana and that
                it is formatted '{self.hubmap_id}' with no surrounding whitespace.
                """
            )
            return ""
        return task_id

    @cached_property
    def get_asana_status(self) -> str:
        """
        Maps between enum for Entity API status that was passed in
        and Asana status to be set.
        """
        if self.status not in self.asana_status_map:
            raise Exception(
                f"""Status {self.status} assigned to {self.hubmap_id}
                not found in asana_status_map. Status not updated."""
            )
        asana_status = self.asana_status_map[self.status]
        return asana_status

    def update_process_stage(self) -> None:
        if self.status in [None, Statuses.UPLOAD_PROCESSING, Statuses.DATASET_PROCESSING]:
            return
        elif self.status == Statuses.UPLOAD_REORGANIZED:
            self.create_dataset_cards()
        body = {"data": {"custom_fields": {PROCESS_STAGE_FIELD_GID: self.get_asana_status}}}
        try:
            response = self.tasks_client.update_task(body, self.get_task_by_hubmap_id)
            assert isinstance(response, TaskResponseData)
            self.check_returned_status(response)
        except ApiException as e:
            logging.info(
                f"""
                    Error occurred while updating Asana status for HuBMAP ID {self.hubmap_id}.
                    Task status not updated, may need to be updated manually.
                    Error: {e}
                """
            )

    def check_returned_status(self, response: TaskResponseData) -> None:
        try:
            new_status = list(
                field["enum_value"]
                for field in response.to_dict()["data"]["custom_fields"]
                if field["name"] == "Process Stage"
            )[0]
            assert (
                self.get_asana_status == new_status["enum_value"]["gid"]
            ), f"""
                Asana status matching Entity API status '{self.status}' not applied
                to {self.hubmap_id}. Current status in Asana matches GID {new_status['name']}.
                """
            logging.info(f"UPDATE SUCCESSFUL: {response}")
        except Exception as e:
            raise Exception(
                f"""Error occurred while updating Asana status for HuBMAP ID {self.hubmap_id}.
                Status not updated.
                Error: {e}"""
            )

    def convert_utc_timestamp(self, dataset: Dict) -> str:
        # Convert UTC timestamp in milliseconds to readable date
        timestamp = datetime.utcfromtimestamp(int(dataset["created_timestamp"]) / 1000).strftime(
            "%Y%m%d"
        )
        return timestamp

    def create_dataset_cards(self) -> None:
        child_datasets = [dataset for dataset in self.submission_data["datasets"]]
        logging.info(
            f"""Upload {self.hubmap_id} has {len(child_datasets)} child datasets.
            Creating Asana cards..."""
        )
        for dataset in child_datasets:
            timestamp = self.convert_utc_timestamp(dataset)
            try:
                response = self.tasks_client.create_task(
                    {
                        "data": {
                            # TODO: should assay type pull from dataset['data_types'] instead?
                            "name": f"{dataset['group_name']} | {dataset['ingest_metadata']['metadata']['assay_type']} | {timestamp}",  # noqa
                            "custom_fields": {
                                HUBMAP_ID_FIELD_GID: dataset["hubmap_id"],
                                PROCESS_STAGE_FIELD_GID: PROCESS_STAGE_GIDS["Intake"],
                                # ENTITY_TYPE_FIELD_GID: ENTITY_TYPE_GIDS[
                                #     "dataset"
                                # ],
                                # TODO: should this be the parent card ID instead?
                                # PARENT_FIELD_GID: self.hubmap_id,
                            },
                            "projects": [self.project],
                        }
                    }
                )
            except ApiException as e:
                raise Exception(
                    f"""Error creating card for dataset {dataset['hubmap_id']},
                    part of reorganized dataset {self.hubmap_id}: {e}"""
                )
            logging.info(
                f"""Card created successfully for dataset {dataset['hubmap_id']}.
                            Response:
                            {response}"""
            )
        logging.info(f"All dataset cards created for upload {self.hubmap_id}")
