import json
import logging
from enum import Enum
from functools import cached_property
from pprint import pprint
from typing import Any, TypedDict

import asana
from status_change.status_utils import get_hubmap_id_from_uuid, get_submission_context

from airflow.providers.http.hooks.http import HttpHook

"""
TODO:
    - Emails?
    - Determine what extra_fields (e.g. validation_message for invalid status(es)) are required for each status and how to manage them
"""


class Statuses(Enum):
    DATASET_ABANDONED = "Abandoned"
    DATASET_APPROVED = "Approved"
    DATASET_ERROR = "Error"
    DATASET_INVALID = "Invalid"
    DATASET_NEW = "New"
    DATASET_PROCESSING = "Processing"
    DATASET_PUBLISHED = "Published"
    DATASET_QA = "QA"
    UPLOAD_ERROR = "Error"
    UPLOAD_INVALID = "Invalid"
    UPLOAD_NEW = "New"
    UPLOAD_PROCESSING = "Processing"
    UPLOAD_REORGANIZED = "Reorganized"
    UPLOAD_SUBMITTED = "Submitted"
    UPLOAD_VALID = "Valid"


class StatusChangerExtras(TypedDict):
    extra_fields: dict[str, Any]
    extra_options: dict[str, Any]


class StatusChangerException(Exception):
    pass


"""
Example usage, default path (update status, update Asana):

    from status_manager import StatusChanger, Statuses

    StatusChanger(
            "uuid_string",
            "token_string",
            Statuses.STATUS_ENUM,
            {
                "extra_fields": {},
                "extra_options": {},
            }
        ).on_status_change()
"""


class StatusChanger:
    def __init__(
        self,
        uuid: str,
        token: str,
        status: Statuses,
        extras: StatusChangerExtras,
        http_conn_id: str = "entity_api_connection",
    ):
        self.uuid = uuid
        self.token = token
        self.status = status
        self.extras = extras
        self.http_conn_id = http_conn_id

    def format_status_data(self) -> dict:
        if not type(self.status) == Statuses:
            raise StatusChangerException(
                f"Status {self.status} for uuid {self.uuid} is not part of the Statuses enum. Status not changed."
            )
        data = {"status": self.status.value}
        # Double-check that you're not accidentally overwriting status
        if (extra_status := self.extras.get("status")) is not None:
            assert (
                extra_status == self.status
            ), f"Entity uuid {self.uuid} passed two different statuses: {self.status} and {extra_status} as part of extras."
        data.update(self.extras["extra_fields"])
        return data

    def set_entity_api_status(self) -> None:
        endpoint = f"/entities/{self.uuid}"
        headers = {
            "authorization": "Bearer " + self.token,
            "X-Hubmap-Application": "ingest-pipeline",
            "content-type": "application/json",
        }
        http_hook = HttpHook("PUT", http_conn_id=self.http_conn_id)
        data = self.format_status_data()
        logging.info(
            f"""
            data:
            {pprint(data)}
            """
        )
        try:
            logging.info(f"Setting status to {data['status']}...")
            extra_options = self.extras["extra_options"].update({"check_response": True})
            response = http_hook.run(endpoint, json.dumps(data), headers, extra_options)
            logging.info(
                f"""
                    Status set to {response.json()['status']}.
                    Response:
                    {response}
                """
            )
        except Exception as e:
            raise StatusChangerException(
                f"Encountered error with request to change status for {self.uuid}, status not set. Error: {e}"
            )

    def update_asana(self) -> None:
        UpdateAsana(self.uuid, self.token, self.status).update_process_stage()

    def send_email(self):
        pass

    status_map = {}
    """
    Default behavior is to call both set_entity_api_status and update_asana.
    Add any statuses to map that require a different process.

    Example:
    {
        # "Statuses.UPLOAD_INVALID": [set_entity_api_status, update_asana, send_email],
        # "Statuses.DATASET_INVALID": [set_entity_api_status, update_asana, send_email],
    }
    """

    def on_status_change(self):
        if self.status in self.status_map:
            for func in self.status_map[self.status]:
                func(self)
        else:
            self.set_entity_api_status()
            self.update_asana()


class UpdateAsana:
    def __init__(self, uuid: str, token: str, status: Statuses):
        # set at least API_KEY as a secret
        self.workspace = WORKSPACE_ID
        self.project = PROJECT_ID
        self.client = asana.Client.access_token(API_KEY)
        self.hubmap_id = get_hubmap_id_from_uuid(token, uuid)
        self.status = status
        self.token = token
        self.submission_data = get_submission_context(token, uuid)
        self.custom_field_gids()

        self.asana_status_map = {
            Statuses.DATASET_ABANDONED: self.process_stage_gids["Blocked 🛑"],
            Statuses.DATASET_APPROVED: self.process_stage_gids["Publishing"],
            Statuses.DATASET_ERROR: self.process_stage_gids["Blocked 🛑"],
            Statuses.DATASET_INVALID: self.process_stage_gids["Blocked 🛑"],
            Statuses.DATASET_PUBLISHED: self.process_stage_gids["Publishing"],
            Statuses.DATASET_QA: self.process_stage_gids["Post-Processing"],
            Statuses.UPLOAD_ERROR: self.process_stage_gids["Blocked 🛑"],
            Statuses.UPLOAD_INVALID: self.process_stage_gids["Blocked 🛑"],
            Statuses.UPLOAD_NEW: self.process_stage_gids["Intake"],
            Statuses.UPLOAD_REORGANIZED: self.process_stage_gids["Processing"],
            Statuses.UPLOAD_SUBMITTED: self.process_stage_gids["Pre-Processing"],
            Statuses.UPLOAD_VALID: self.process_stage_gids["Pre-Processing"],
        }

    # @cached_property
    # def get_sibling_datasets(self) -> list:
    #     # @note need to check on how derived datasets factor in
    #     """
    #     Should only be called for entity_type "Dataset".
    #     Check first whether the Asana task is labeled "Datasets".
    #     If so, retrieve other datasets in the "HuBMAP ID" field.
    #     If not, first retrieve parent upload from Entity API and
    #     then query its child datasets.
    #     """
    #     sibling_datasets = []
    #     status = ""
    #     task = self.client.tasks.get_task(self.hubmap_id_field)
    #     for field in task["custom_fields"]:
    #         # Using Status/In progress as a stand-in for the entity type field
    #         if field["name"] == "Status":
    #             status = field["enum_value"]
    #     # if status == "Dataset":
    #     if status == "In progress":
    #         for field in task["custom_fields"]:
    #             if field["name"] == "HuBMAP ID":
    #                 sibling_datasets = field["text_value"].split(" ")
    #                 return sibling_datasets
    #     else:
    #         try:
    #             parent_upload_uuid = get_submission_context(
    #                 self.token, self.submission_data["upload"]["uuid"]
    #             ).get("uuid")
    #             if parent_upload_uuid:
    #                 sibling_dataset_info = get_submission_context(self.token, parent_upload_uuid)
    #                 for dataset in sibling_dataset_info:
    #                     sibling_datasets.append(dataset.get("hubmap_id"))
    #             return sibling_datasets
    #         except Exception as e:
    #             raise StatusChangerException(
    #                 f"Retrieving sibling datasets from Entity API for {self.hubmap_id} failed! Error: {e}"
    #             )
    #     return []
    #
    # def progress_task(self) -> bool:
    #     """
    #     Check dataset statuses first to see if any are in a status that
    #     would block the Asana task from being updated. If not, check whether
    #     the status to be changed to is Approved/Published. In that case, make
    #     sure all sibling datasets are also through QA.
    #     """
    #     sibling_datasets = self.get_sibling_datasets
    #     for dataset in sibling_datasets:
    #         status = get_submission_context(self.token, dataset).get("status")
    #         if status in [
    #             Statuses.DATASET_PROCESSING.value,
    #             Statuses.DATASET_ERROR.value,
    #             Statuses.DATASET_INVALID.value,
    #             Statuses.DATASET_NEW.value,
    #         ]:
    #             return False
    #         elif self.status in [Statuses.DATASET_APPROVED, Statuses.DATASET_PUBLISHED]:
    #             if status == Statuses.DATASET_QA:
    #                 return False

    def custom_field_gids(self) -> None:
        response = self.client.custom_field_settings.get_custom_field_settings_for_project(
            self.project
        )
        self.process_stage_gids = {}
        for custom_field in response:
            try:
                name = custom_field["custom_field"]["name"]
            except Exception:
                continue
            if name == "HuBMAP ID":
                self.hubmap_id_gid = custom_field["custom_field"]["gid"]
            elif name == "Process Stage":
                self.process_stage_field_gid = custom_field["custom_field"]["gid"]
                for stage in custom_field["custom_field"]["enum_options"]:
                    self.process_stage_gids[stage["name"]] = stage["gid"]
        if (
            not self.process_stage_gids
            or not self.hubmap_id_gid
            or not self.process_stage_field_gid
        ):
            raise Exception(
                f"""
                            Unable to retrieve some GIDs from Asana board {self.project}:
                            Process Stage GIDs: {self.process_stage_gids}
                            Process Stage field GID: {self.process_stage_field_gid}
                            HuBMAP ID field GID: {self.hubmap_id_gid}
                            """
            )

    @cached_property
    def get_task_by_hubmap_id(self) -> str:
        """
        Given a HuBMAP ID, find the associated Asana task.
        Fails if a HuBMAP ID is associated with 0 or >1 tasks.
        """
        response = self.client.tasks.search_tasks_for_workspace(
            self.workspace,
            {
                "projects_any": self.project,
                f"custom_fields.{self.hubmap_id_gid}.contains": self.hubmap_id,
            },
        )
        try:
            response_list = [item for item in response]
            response_length = len(list(response_list))
        except Exception as e:
            raise StatusChangerException(
                f"Error occurred while retrieving Asana task for {self.hubmap_id}. Error: {e}"
            )
        if response_length > 1:
            task_gids = [task["gid"] for task in response_list]
            raise StatusChangerException(
                f"""
                Error retrieving task by HuBMAP ID for {self.hubmap_id}! ID is associated with multiple Asana tasks: {task_gids}.
                """
            )
        elif response_length == 0:
            raise StatusChangerException(
                f"""
                Error retrieving task by HuBMAP ID for {self.hubmap_id}! No results found.
                Check that a task for the expected HuBMAP ID exists in Asana and that it is formatted '{self.hubmap_id}' with no surrounding whitespace.
                """
            )
        task_id = response_list[0]["gid"]
        return task_id

    @cached_property
    def get_asana_status(self) -> str:
        """
        Maps between enum for Entity API status that was passed in
        and Asana status to be set.
        """
        if self.status not in self.asana_status_map:
            raise StatusChangerException(
                f"Status {self.status} assigned to {self.hubmap_id} not found in asana_status_map. Status not updated."
            )
        asana_status = self.asana_status_map[self.status]
        return asana_status

    def update_process_stage(self) -> None:
        # if (self.submission_data["entity_type"] == "Dataset") and (self.progress_task() is False):
        #     try:
        #         notes_field = self.client.tasks.get_task(self.get_task_by_hubmap_id).get("notes")
        #         response = self.client.tasks.update_task(
        #             self.get_task_by_hubmap_id,
        #             {
        #                 "notes": notes_field
        #                 + f"{self.hubmap_id} set to {self.status.value} on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        #             },
        #         )
        #         logging.info(
        #             f"""Notes for task {self.get_task_by_hubmap_id} updated. Process Stage for task not updated because all datasets are not yet in appropriate statuses.
        #             Response: {response}
        #             """
        #         )
        #         return
        #     except Exception as e:
        #         raise StatusChangerException(
        #             f"""Error updating notes for task {self.get_task_by_hubmap_id} occurred while checking sibling dataset statuses for {self.hubmap_id}.
        #                         Error: {e}
        #                         """
        #         )
        # logging.info(
        #     f"Dataset {self.hubmap_id} and all sibling datasets are in state {self.status} or are further along in process; updating Process Stage for task {self.get_task_by_hubmap_id} to {self.get_asana_status}."
        # )
        try:
            response = self.client.tasks.update_task(
                self.get_task_by_hubmap_id,
                {"custom_fields": {self.process_stage_gid: self.get_asana_status}},
                opt_pretty=True,
            )
            logging.info(f"UPDATE SUCCESSFUL: {response}")
        except Exception as e:
            raise StatusChangerException(
                f"Error occurred while updating HuBMAP ID {self.hubmap_id}; not updated. Error: {e}"
            )
