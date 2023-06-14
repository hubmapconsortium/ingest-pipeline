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
    - Add dag to run nightly to check statuses
    - Emails?
    - Determine what extra_fields (e.g. validation_message for invalid status(es)) are required for each status and how to manage them
"""


class Statuses(Enum):
    # Dataset Hold and Deprecated are not currently in use but are valid for Entity API
    DATASET_DEPRECATED = "Deprecated"
    DATASET_ERROR = "Error"
    DATASET_HOLD = "Hold"
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
            },
            #optional http_conn_id="entity_api_connection"
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
        verbose: bool = True,
    ):
        self.uuid = uuid
        self.token = token
        self.status = status
        self.extras = extras
        self.http_conn_id = http_conn_id
        self.verbose = verbose

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
            if self.verbose:
                logging.info(f"Setting status to {data['status']}...")
            extra_options = self.extras["extra_options"].update({"check_response": True})
            response = http_hook.run(endpoint, json.dumps(data), headers, extra_options)
            if self.verbose:
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

        # If any Process Stage enum values change in Asana, update them here
        self.asana_status_map = {
            Statuses.DATASET_ERROR: self.process_stage_gids["Blocked 🛑"],
            Statuses.DATASET_INVALID: self.process_stage_gids["Blocked 🛑"],
            Statuses.DATASET_NEW: self.process_stage_gids["Intake"],
            Statuses.DATASET_PUBLISHED: self.process_stage_gids["Publishing"],
            Statuses.DATASET_QA: self.process_stage_gids["Post-Processing"],
            Statuses.UPLOAD_ERROR: self.process_stage_gids["Blocked 🛑"],
            Statuses.UPLOAD_INVALID: self.process_stage_gids["Blocked 🛑"],
            Statuses.UPLOAD_NEW: self.process_stage_gids["Intake"],
            Statuses.UPLOAD_REORGANIZED: self.process_stage_gids["Processing"],
            Statuses.UPLOAD_SUBMITTED: self.process_stage_gids["Pre-Processing"],
            Statuses.UPLOAD_VALID: self.process_stage_gids["Pre-Processing"],
        }

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
            # possible to use data dict here instead?
            if name == "HuBMAP ID":
                self.hubmap_id_gid = custom_field["custom_field"]["gid"]
            elif name == "Process Stage":
                self.process_stage_field_gid = custom_field["custom_field"]["gid"]
                for stage in custom_field["custom_field"]["enum_options"]:
                    self.process_stage_gids[stage["name"]] = stage["gid"]
            # check naming of these fields in Asana
            elif name == "Parent":
                self.parent_field_gid = custom_field["custom_field"]["gid"]
            elif name == "Entity Type":
                self.entity_type_field_gid = custom_field["custom_field"]["gid"]
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
        # TODO: this might need to change for grouped dataset tasks--HuBMAP ID would be present in two cards
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
        if self.status == Statuses.UPLOAD_REORGANIZED:
            self.create_dataset_cards()
        try:
            response = self.client.tasks.update_task(
                self.get_task_by_hubmap_id,
                {"custom_fields": {self.process_stage_field_gid: self.get_asana_status}},
                opt_pretty=True,
            )
            logging.info(f"UPDATE SUCCESSFUL: {response}")
        except Exception as e:
            raise StatusChangerException(
                f"Error occurred while updating HuBMAP ID {self.hubmap_id}; not updated. Error: {e}"
            )

    def create_dataset_cards(self):
        # TODO: assign dataset hubmap ids to list and count
        logging.info(f"Upload {self.hubmap_id} has n child datasets. Creating Asana cards...")
        try:
            # for dataset_id in child_datasets:
            # get fields for title (should be in self.submission_data)
            response = self.client.tasks.create_task(
                {
                    "custom_fields": {
                        # "title": "Data Provider institution | assay type | submission date"
                        # self.hubmap_id_gid: dataset_id,
                        # TODO: is "New" correct? Should it be mapped to Intake?
                        self.process_stage_field_gid: "New",
                        self.entity_type_field_gid: "Dataset",
                        self.parent_field_gid: self.hubmap_id,
                        # TODO: does parent_field_gid need to be extensible for card IDs for non-upload groups? is that a separate field or is this actually a parent milestone ID field?
                    },
                    "projects": [self.project],
                },
                opt_pretty=True,
            )
            # logging.info(f"Card created successfully for dataset {dataset_id}")
        except Exception:
            raise StatusChangerException(
                # f"Error creating dataset card for dataset {hubmap_id}, part of reorganized dataset {self.hubmap_id}"
            )
        logging.info(f"All dataset cards created for upload {self.hubmap_id}")
