import logging
from datetime import datetime
from functools import cached_property
from typing import Dict, Optional

import asana
from asana.models.task_response_data import TaskResponseData
from asana.rest import ApiException

from airflow.configuration import conf as airflow_conf

from .status_utils import Statuses, get_hubmap_id_from_uuid, get_submission_context

HUBMAP_ID_FIELD_GID = "1204584344373110"
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
ENTITY_TYPE_FIELD_GID = ""
ENTITY_TYPE_GIDS = {}


class AsanaException(Exception):
    pass


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
            Statuses.DATASET_SUBMITTED: PROCESS_STAGE_GIDS["Intake"],
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
            raise AsanaException("Cannot fetch data from Entity API. No uuid passed.")

    def get_task_by_hubmap_id(self) -> Optional[str]:
        """
        Given a HuBMAP ID, find the associated Asana task.
        Fails if a HuBMAP ID is associated with 0 or >1 tasks.
        """
        custom_field = f"custom_fields_{HUBMAP_ID_FIELD_GID}_contains"
        response_list = self.tasks_client.search_tasks_for_workspace(
            self.workspace, **{custom_field: self.hubmap_id}
        )
        response_length = len(response_list.data)
        if response_length == 1:
            task_id = response_list.data[0].gid
            return task_id
        elif response_length != 1:
            tasks = {}
            for response in response_list.data:
                if response.gid:
                    tasks[response.name] = response.gid
            raise AsanaException(
                f"""{response_length} tasks with the HuBMAP ID {self.hubmap_id} found!
                {f'Task Names & GIDs found: {tasks}'
                 if tasks
                 else f'''Check that a task for the expected HuBMAP ID
                 exists in Asana and that it is formatted '{self.hubmap_id}'
                 with no surrounding whitespace.'''}
                """
            )

    @cached_property
    def get_asana_status(self) -> str:
        """
        Maps between enum for Entity API status that was passed in
        and Asana status to be set.
        """
        if self.status not in self.asana_status_map:
            raise AsanaException(
                f"""Status {self.status} assigned to {self.hubmap_id}
                not found in asana_status_map. Status not updated."""
            )
        asana_status = self.asana_status_map[self.status]
        return asana_status

    def update_process_stage(self) -> None:
        if self.status in [None, Statuses.UPLOAD_PROCESSING, Statuses.DATASET_PROCESSING]:
            return
        elif self.status == Statuses.DATASET_PUBLISHED:
            self.mark_subtask_complete()
            return
        body = {"data": {"custom_fields": {PROCESS_STAGE_FIELD_GID: self.get_asana_status}}}
        try:
            response = self.tasks_client.update_task(body, self.get_task_by_hubmap_id())
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
        if self.status == Statuses.UPLOAD_REORGANIZED:
            # parent_task = response.json()["gid"]
            # self.create_dataset_cards(parent_task)
            pass

    def check_returned_status(self, response: TaskResponseData) -> None:
        try:
            new_status = [
                field["enum_value"]
                for field in response.to_dict()["data"]["custom_fields"]
                if field["name"] == "Process Stage"
            ][0]
            assert (
                self.get_asana_status == new_status["gid"]
            ), f"""
                Asana status matching Entity API status '{self.status}' not applied
                to {self.hubmap_id}. Current status in Asana matches GID {new_status['name']}.
                """
            logging.info(f"UPDATE SUCCESSFUL: {response}")
        except Exception as e:
            raise AsanaException(
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

    def create_dataset_cards(self, parent_task: str) -> None:
        child_datasets = [dataset for dataset in self.submission_data["datasets"]]
        logging.info(
            f"""Upload {self.hubmap_id} has {len(child_datasets)} child datasets:
                {[dataset["uuid"] for dataset in child_datasets]}
            Creating Asana cards..."""
        )
        for dataset in child_datasets:
            timestamp = self.convert_utc_timestamp(dataset)
            data_types = ", ".join(dataset["data_types"])
            body = asana.TaskGidSubtasksBody(
                {
                    "name": f"{dataset['group_name']} | {data_types} | {timestamp}",
                    "custom_fields": {
                        HUBMAP_ID_FIELD_GID: dataset["hubmap_id"],
                        PROCESS_STAGE_FIELD_GID: PROCESS_STAGE_GIDS["Intake"],
                        # ENTITY_TYPE_FIELD_GID: ENTITY_TYPE_GIDS[
                        #     "dataset"
                        # ],
                    },
                    "projects": [self.project],
                }
            )
            try:
                # TODO: Asana board needs to filter out datasets
                response = self.tasks_client.create_subtask_for_task(
                    body,
                    task_gid=parent_task,
                )
            except ApiException as e:
                raise AsanaException(
                    f"""Error creating card for dataset {dataset['hubmap_id']},
                    part of reorganized dataset {self.hubmap_id}: {e}"""
                )
            logging.info(
                f"""Card created successfully for dataset {dataset['hubmap_id']}.
                            Response:
                            {response}"""
            )
        logging.info(f"All dataset cards created for upload {self.hubmap_id}")

    def mark_subtask_complete(self):
        """
        TODO: this depends on how DATASET_PUBLISHED is set
        The thinking is that all datasets could be set as
        subtasks and could be checked off as they are published,
        with an Asana rule that moves an upload card to
        Publishing after subtasks are checked off.
        """
        pass
