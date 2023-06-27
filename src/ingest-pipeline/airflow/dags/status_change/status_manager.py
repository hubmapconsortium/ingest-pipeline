import json
import logging
import os
from datetime import datetime
from enum import Enum
from functools import cached_property
from pprint import pprint
from typing import Any, TypedDict

import asana

# from status_change.status_utils import get_hubmap_id_from_uuid, get_submission_context
from status_utils import get_hubmap_id_from_uuid, get_submission_context

from airflow.providers.http.hooks.http import HttpHook

"""
TODO:
    - Add dag to run nightly to check statuses
    - Emails?
    - Determine what extra_fields (e.g. validation_message for invalid status(es)) are required for each status and how to manage them
"""


class Statuses(str, Enum):
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


DATASET_STATUS_MAP = {
    "Deprecated": Statuses.DATASET_DEPRECATED,
    "Error": Statuses.DATASET_ERROR,
    "Hold": Statuses.DATASET_HOLD,
    "Invalid": Statuses.DATASET_INVALID,
    "New": Statuses.DATASET_NEW,
    "Processing": Statuses.DATASET_PROCESSING,
    "Published": Statuses.DATASET_PUBLISHED,
    "QA": Statuses.DATASET_QA,
}

UPLOAD_STATUS_MAP = {
    "Error": Statuses.UPLOAD_ERROR,
    "Invalid": Statuses.UPLOAD_INVALID,
    "New": Statuses.UPLOAD_NEW,
    "Processing": Statuses.UPLOAD_PROCESSING,
    "Reorganized": Statuses.UPLOAD_REORGANIZED,
    "Submitted": Statuses.UPLOAD_SUBMITTED,
    "Valid": Statuses.UPLOAD_VALID,
}


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
        status: Statuses | None,
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
        data = {}
        if self.status is None:
            pass
        elif type(self.status) == Statuses:
            data["status"] = self.status
        else:
            raise StatusChangerException(
                f"Status {self.status} for uuid {self.uuid} is not part of the Statuses enum. Status not changed."
            )
        # Double-check that you're not accidentally overwriting status
        if (extra_status := self.extras.get("status")) is not None:
            assert (
                extra_status == self.status
            ), f"Entity uuid {self.uuid} passed two different statuses: {self.status} and {extra_status} as part of extras."
        data.update(self.extras["extra_fields"])
        return data

    def set_entity_api_status(self) -> dict:
        endpoint = f"/entities/{self.uuid}"
        headers = {
            "authorization": "Bearer " + self.token,
            "X-Hubmap-Application": "ingest-pipeline",
            "content-type": "application/json",
        }
        http_hook = HttpHook("PUT", http_conn_id=self.http_conn_id)
        data = self.format_status_data()
        if self.extras["extra_options"].get("check_response") is None:
            self.extras["extra_options"].update({"check_response": True})
        logging.info(
            f"""
            data:
            {pprint(data)}
            """
        )
        try:
            if self.verbose and data.get("status") is not None:
                logging.info(f"Setting status to {data['status']}...")
            response = http_hook.run(
                endpoint, json.dumps(data), headers, self.extras["extra_options"]
            )
            if self.verbose:
                logging.info(
                    f"""
                        Status set to {response.json()['status']}.
                        Response:
                    """
                )
            return response.json()
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
    def __init__(
        self,
        uuid: str | None,
        token: str,
        status: Statuses | None,
        workspace_id: str = os.environ["WORKSPACE_ID"],  # Set as secret
        project_id: str = os.environ["PROJECT_ID"],  # Set as secret
    ):
        self.workspace = workspace_id
        self.project = project_id
        self.client = asana.Client.access_token(os.environ["ASANA_API_KEY"])  # Set as secret
        self.status = status
        self.token = token
        self.uuid = uuid
        self.custom_fields = self.get_custom_fields()
        self.process_stage_field_gid = self.get_custom_field_gid("Process Stage")

    @property
    def asana_status_map(self):
        return {
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

    @cached_property
    def hubmap_id(self):
        if self.uuid is not None:
            return get_hubmap_id_from_uuid(self.token, self.uuid)

    @cached_property
    def submission_data(self):
        if self.uuid is not None:
            return get_submission_context(self.token, self.uuid)

    def get_custom_fields(self):
        return list(
            self.client.custom_field_settings.get_custom_field_settings_for_project(self.project)
        )

    def get_custom_field_gid(self, custom_field_name: str) -> str:
        for custom_field in self.custom_fields:
            try:
                name = custom_field["custom_field"]["name"]
            except Exception:
                continue
            if name == custom_field_name:
                if name == "Process Stage":
                    self.process_stage_gids = {}
                    for stage in custom_field["custom_field"]["enum_options"]:
                        self.process_stage_gids[stage["name"]] = stage["gid"]
                elif name == "Entity Type":
                    self.entity_type_gids = {}
                    for stage in custom_field["custom_field"]["enum_options"]:
                        self.process_stage_gids[stage["name"]] = stage["gid"]
                return custom_field["custom_field"]["gid"]
        raise Exception(f"Asana custom field {custom_field_name} not found.")

    @cached_property
    def get_task_by_hubmap_id(self) -> str:
        """
        Given a HuBMAP ID, find the associated Asana task.
        Fails if a HuBMAP ID is associated with 0 or >1 tasks.
        """
        response_list = list(
            self.client.tasks.search_tasks_for_workspace(
                self.workspace,
                {
                    "projects_any": self.project,
                    f"custom_fields.{self.get_custom_field_gid('HuBMAP ID')}.contains": self.hubmap_id,
                    "opt_fields": ["gid", "custom_fields"],
                },
            )
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
                raise StatusChangerException(
                    f"Multiple tasks with the entity_type tag 'Dataset' found for HuBMAP ID {self.hubmap_id}. GIDs found: {task_gids}"
                )
            elif len(types) == 0:
                raise StatusChangerException(
                    f"No tasks with type 'Dataset' found for HuBMAP ID {self.hubmap_id}."
                )
            task_id = gids[0]
        else:
            raise StatusChangerException(
                f"""
                Error retrieving task by HuBMAP ID for {self.hubmap_id}! {response_length} results found.
                Check that a task for the expected HuBMAP ID exists in Asana and that it is formatted '{self.hubmap_id}' with no surrounding whitespace.
                """
            )
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
        if self.status is None:
            return
        elif self.status == Statuses.UPLOAD_REORGANIZED:
            self.create_dataset_cards()
        try:
            response = self.client.tasks.update_task(
                self.get_task_by_hubmap_id,
                {
                    "custom_fields": {
                        self.get_custom_field_gid("Process Stage"): self.get_asana_status
                    },
                },
                opt_pretty=True,
            )
            new_status = None
            for custom_field in response["custom_fields"]:
                if custom_field["name"] == "Status":
                    new_status = custom_field["enum_value"]["gid"]
            assert (
                self.get_asana_status == new_status
            ), f"Asana status matching Entity API status '{self.status}' not applied to {self.hubmap_id}. Current status in Asana is {new_status}."
            logging.info(f"UPDATE SUCCESSFUL: {response}")
        except Exception as e:
            raise StatusChangerException(
                f"Error occurred while updating HuBMAP ID {self.hubmap_id}; not updated. Error: {e}"
            )

    def convert_utc_timestamp(self, dataset):
        # Convert UTC timestamp in milliseconds to readable date
        timestamp = datetime.utcfromtimestamp(int(dataset["created_timestamp"]) / 1000).strftime(
            "%Y%m%d"
        )
        return timestamp

    def create_dataset_cards(self):
        submission_data = self.submission_data()
        child_datasets = [dataset for dataset in submission_data["datasets"]]
        logging.info(
            f"Upload {self.hubmap_id} has {len(child_datasets)} child datasets. Creating Asana cards..."
        )
        for dataset in child_datasets:
            timestamp = self.convert_utc_timestamp(dataset)
            try:
                response = self.client.tasks.create_task(
                    {
                        "name": f"{dataset['group_name']} | {dataset['ingest_metadata']['metadata']['assay_type']} | {timestamp}",
                        "custom_fields": {
                            # TODO: should assay type pull from dataset['data_types'] (e.g. SNARE-RNAseq1) or dataset['ingest_metadata']['metadata']['assay_type'] (e.g. sciRNAseq)?
                            self.get_custom_field_gid("HuBMAP ID"): dataset["hubmap_id"],
                            # TODO: is "New" correct? Should it be mapped to Intake?
                            self.get_custom_field_gid("Process Stage"): self.process_stage_gids[
                                "Intake"
                            ],
                            # self.get_custom_field_gid("Entity Type"): self.entity_type_gids[
                            #     "dataset"
                            # ],
                            # # TODO: does parent_field_gid need to be extensible for card IDs for non-upload groups? is that a separate field or is this actually a parent milestone ID field?
                            # self.get_custom_field_gid("Parent"): self.hubmap_id,
                        },
                        "projects": [self.project],
                    },
                    opt_pretty=True,
                )
                response.raise_for_status()
                logging.info(
                    f"""Card created successfully for dataset {dataset['hubmap_id']}.
                             Response:
                             {response.json()}"""
                )
            except Exception:
                raise StatusChangerException(
                    f"Error creating card for dataset {dataset['hubmap_id']}, part of reorganized dataset {self.hubmap_id}"
                )
        logging.info(f"All dataset cards created for upload {self.hubmap_id}")
