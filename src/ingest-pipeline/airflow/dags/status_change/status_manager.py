import json
import logging
from enum import Enum
from functools import cached_property
from pprint import pprint
from typing import Any, TypedDict

import asana
from status_utils import get_hubmap_id_from_uuid

from airflow.providers.http.hooks.http import HttpHook

"""
TODO:
    - Refactor validate_upload to work as test case
    - Emails?
    - Determine what extra_fields (e.g. validation_message for invalid status(es)) are required
      required for each status and how to manage them
    - Write tests
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


"""
Example usage, default path (update status, update Asana):

    from status_manager import StatusChanger, StatusChangerExtras, Statuses

    StatusChanger(
            "uuid_string",
            "token_string",
            Statuses.STATUS_ENUM,
            status_changer_extras: StatusChangerExtras = {
                "extra_fields": {},
                "extra_options": {},
            }
"""


class StatusChanger:
    def __init__(
        self,
        uuid: str,
        token: str,
        status: Statuses,
        extras: StatusChangerExtras,
        # @note: Need to change default in production
        env: str = "dev",
    ):
        self.uuid = uuid
        self.token = token
        self.status = status
        self.extras = extras
        if env == "prod":
            self.http_conn_id = "entity_api_connection"
        elif env in ["dev", "test"]:
            self.http_conn_id = f"https://entity-api.{env}.hubmapconsortium.org"
        else:
            raise Exception(f"Unknown environment {env} passed to StatusChanger.")

    def format_status_data(self):
        data = {
            "status": self.status.value,
        }
        for key, value in self.extras["extra_fields"]:
            data[key] = value
        return data

    def set_entity_api_status(self):
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
            logging.info(f"Setting status to {self.status.value}...")
            response = http_hook.run(
                endpoint, json.dumps(data), headers, self.extras["extra_options"]
            )
            response.check_response()
            logging.info(
                f"""
                    Status set to {response.json()['status']}.
                    Response:
                    {response}
                """
            )
        except Exception as e:
            logging.info(f"Encountered error, status not set. Error: {e}")
            raise

    def update_asana(self):
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


# maybe fragile, could retrieve programmatically
# but would still need a map for names (i.e. "Ready Backlog" to READY_BACKLOG)
# which is fragile in a different way
# @note: gids are for test Asana project
class AsanaProcessStage(Enum):
    INTAKE = "1204584344373115"
    PRE_PROCESSING = "1204584344373116"
    READY_BACKLOG = "1204584347884579"
    PROCESSING = "1204584347884580"
    POST_PROCESSING = "1204584347884581"
    PUBLISHING = "1204584347884582"
    BLOCKED = "1204584347884583"
    COMPLETED = "1204584347884584"
    ABANDONED = ""


class UpdateAsana:
    def __init__(self, uuid: str, token: str, status: Statuses):
        self.http_conn_id = "https://app.asana.com/api/1.0"
        # self.workspace = "830534504524868"
        # self.project = "1204583312696119"
        self.workspace = WORKSPACE_ID
        self.project = PROJECT_ID
        self.client = asana.Client.access_token(API_KEY)
        self.hubmap_id = get_hubmap_id_from_uuid(token, uuid)
        self.status = status

    asana_status_map = {
        Statuses.DATASET_ABANDONED: AsanaProcessStage.ABANDONED,
        Statuses.DATASET_ERROR: AsanaProcessStage.BLOCKED,
        Statuses.DATASET_INVALID: AsanaProcessStage.BLOCKED,
        Statuses.DATASET_QA: AsanaProcessStage.POST_PROCESSING,
        Statuses.UPLOAD_ERROR: AsanaProcessStage.BLOCKED,
        Statuses.UPLOAD_INVALID: AsanaProcessStage.BLOCKED,
        Statuses.UPLOAD_NEW: AsanaProcessStage.INTAKE,
        Statuses.UPLOAD_REORGANIZED: AsanaProcessStage.PROCESSING,
        Statuses.UPLOAD_SUBMITTED: AsanaProcessStage.PRE_PROCESSING,
        Statuses.UPLOAD_VALID: AsanaProcessStage.PRE_PROCESSING,
    }

    # asana_status_map = {
    #     AsanaProcessStage.INTAKE: [Statuses.UPLOAD_NEW],
    #     AsanaProcessStage.READY_BACKLOG: [],
    #     AsanaProcessStage.PROCESSING: [Statuses.UPLOAD_REORGANIZED],
    #     AsanaProcessStage.PRE_PROCESSING: [Statuses.UPLOAD_VALID, Statuses.UPLOAD_SUBMITTED],
    #     AsanaProcessStage.POST_PROCESSING: [Statuses.DATASET_QA],
    #     AsanaProcessStage.BLOCKED: [
    #         Statuses.UPLOAD_ERROR,
    #         Statuses.UPLOAD_INVALID,
    #         Statuses.DATASET_ERROR,
    #         Statuses.DATASET_INVALID,
    #     ],
    #     # TODO: All datasets required to be in published or completed for this to happen
    #     # Needs more thought
    #     # AsanaProcessStage.PUBLISHING: [Statuses.DATASET_APPROVED],
    #     # AsanaProcessStage.COMPLETED: [Statuses.DATASET_PUBLISHED],
    #     AsanaProcessStage.ABANDONED: [Statuses.DATASET_ABANDONED],
    # }

    # could hard-code HuBMAP ID gid, unsure whether that or the magic
    # string ("HuBMAP ID") here is better
    @cached_property
    def get_hubmap_id_field(self) -> str:
        response = self.client.custom_field_settings.get_custom_field_settings_for_project(
            self.project
        )
        for custom_field in response:
            try:
                name = custom_field["custom_field"]["name"]
            except Exception:
                continue
            if name == "hubmap id":
                return custom_field["custom_field"]["gid"]
        return ""

    @cached_property
    def get_task_by_hubmap_id(self) -> str:
        response = self.client.tasks.search_tasks_for_workspace(
            self.workspace,
            {
                "projects_any": self.project,
                f"custom_fields.{self.get_hubmap_id_field}.value": self.hubmap_id,
            },
        )
        try:
            response_list = [item for item in response]
            response_length = len(list(response_list))
        except Exception as e:
            logging.info(
                f"Error occurred while retrieving milestone for {self.hubmap_id}. Error: {e}"
            )
            return ""
        if response_length != 1:
            logging.info(
                f"""
                Error retrieving task by HuBMAP ID for {self.hubmap_id}! Retrieved {response_length} results.
                Check that a milestone for the expected HuBMAP ID exists in Asana and that it is formatted '{self.hubmap_id}' with no surrounding whitespace.
                """
            )
            return ""
        task_id = response_list[0]["gid"]
        return task_id

    # could also hard-code process stage gid, see note to get_hubmap_id_field
    @cached_property
    def get_process_stage_gid(self) -> str:
        response = self.client.custom_field_settings.get_custom_field_settings_for_project(
            self.project
        )
        process_stage_gid = [
            custom_field["custom_field"]["gid"]
            for custom_field in response
            if custom_field["custom_field"]["name"] == "process stage"
        ]
        if len(process_stage_gid) != 1:
            logging.info(
                f"Error retrieving process stage gid for HuBMAP ID {self.hubmap_id}; retrieved {len(process_stage_gid)} gids"
            )
        return process_stage_gid[0]

    @property
    def get_asana_status(self):
        if self.status not in self.asana_status_map:
            raise Exception(f"Status for {self.hubmap_id} not found in asana_status_map")
        asana_status = self.asana_status_map[self.status]
        return asana_status.value

    def update_process_stage(self):
        try:
            response = self.client.tasks.update_task(
                self.get_task_by_hubmap_id,
                {"custom_fields": {self.get_process_stage_gid: self.get_asana_status}},
                opt_pretty=True,
            )
            logging.info(f"UPDATE SUCCESSFUL: {response}")
        except Exception as e:
            logging.info(
                f"Error occurred while updating HuBMAP ID {self.hubmap_id}; not updated. Error: {e}"
            )
