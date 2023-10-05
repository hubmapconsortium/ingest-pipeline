from __future__ import annotations

import json
import logging
from enum import Enum
from typing import Any, Dict, TypedDict

from status_change.send_emails import SendFailureEmail
from status_manager.status_utils import get_submission_context

from airflow.providers.http.hooks.http import HttpHook


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


# Needed some way to disambiguate statuses shared by datasets and uploads
ENTITY_STATUS_MAP = {
    "Dataset": {
        "Deprecated": Statuses.DATASET_DEPRECATED,
        "Error": Statuses.DATASET_ERROR,
        "Hold": Statuses.DATASET_HOLD,
        "Invalid": Statuses.DATASET_INVALID,
        "New": Statuses.DATASET_NEW,
        "Processing": Statuses.DATASET_PROCESSING,
        "Published": Statuses.DATASET_PUBLISHED,
        "QA": Statuses.DATASET_QA,
    },
    "Upload": {
        "Error": Statuses.UPLOAD_ERROR,
        "Invalid": Statuses.UPLOAD_INVALID,
        "New": Statuses.UPLOAD_NEW,
        "Processing": Statuses.UPLOAD_PROCESSING,
        "Reorganized": Statuses.UPLOAD_REORGANIZED,
        "Submitted": Statuses.UPLOAD_SUBMITTED,
        "Valid": Statuses.UPLOAD_VALID,
    },
}


class StatusChangerExtras(TypedDict):
    extra_fields: dict[str, Any]
    extra_options: dict[str, Any]


class StatusChangerException(Exception):
    pass


"""
Example usage, simple path (e.g. status string, no validation message):
    from status_manager import StatusChanger
    StatusChanger(
            "uuid_string",
            "token_string",
            "status",
        ).on_status_change()

Example usage, optional params path:
    from status_manager import StatusChanger, Statuses
    StatusChanger(
            "uuid_string",
            "token_string",
            Statuses.STATUS_ENUM or "status",
            # optional {
                "extra_fields": {},
                "extra_options": {},
            },
            #optional entity_type="Dataset"|"Upload"
            #optional http_conn_id="entity_api_connection"
            #optional notification_instance=SendEmail(context)
        ).on_status_change()
"""


class StatusChanger:
    def __init__(
        self,
        uuid: str,
        token: str,
        # TODO: status is currently required; should it be possible
        # to add extra info without updating status?
        status: Statuses | str,
        extras: StatusChangerExtras | None = None,
        entity_type: str | None = None,
        http_conn_id: str = "entity_api_connection",
        # notification_instance assumes email, but could be generalized
        notification_instance: SendFailureEmail | None = None,
        verbose: bool = True,
    ):
        self.uuid = uuid
        self.token = token
        self.http_conn_id = http_conn_id
        self.verbose = verbose
        self.status = (
            status if isinstance(status, Statuses) else self.get_status(status, entity_type)
        )
        self.extras = (
            extras
            if extras
            else {
                "extra_fields": {},
                "extra_options": {},
            }
        )
        self.notification_instance = notification_instance if notification_instance else None

    # TODO: consider checking current status here, as I believe ingest-pipeline gets mad
    # if you try to set an entity's status to the status it already has
    # This would mean that every instance makes a call to entity-api.
    # The alternative is to check the current status just before the set_entity_api_status call.
    def get_status(self, status: str, entity_type: str | None) -> Statuses:
        if entity_type is None:
            try:
                entity_data = get_submission_context(self.token, self.uuid)
                # TODO: check this key and casing for entity_type
                entity_type = entity_data["entity_type"]
                assert entity_type is not None
            except KeyError as e:
                raise StatusChangerException(
                    f"""
                    Could not reconcile entity type for {self.uuid} with status '{status}'.
                    Error {e}
                    """
                )
        try:
            entity_status = ENTITY_STATUS_MAP[entity_type.title()][status]
        except KeyError:
            raise StatusChangerException(
                f"""
                    Could not retrieve status for {self.uuid}.
                    Check that status is valid for entity type.
                    Status not changed.
                """
            )
        return entity_status

    def format_status_data(self) -> Dict[str, str | Dict]:
        data = {}
        data["status"] = self.status
        # Double-check that you're not accidentally overwriting status
        if (extra_status := self.extras.get("status")) is not None:
            assert (
                extra_status == self.status
            ), f"Entity {self.uuid} passed multiple statuses ({self.status} and {extra_status})."
        data.update(self.extras["extra_fields"])
        logging.info(f"COMPILED DATA: {data}")
        return data

    def set_entity_api_status(self) -> Dict:
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
            {data}
            """
        )
        try:
            if self.verbose:
                logging.info(f"Setting status to {data['status']}...")
            response = http_hook.run(
                endpoint, json.dumps(data), headers, self.extras["extra_options"]
            )
            return response.json()
        except Exception as e:
            raise StatusChangerException(
                f"""
                Encountered error with request to change status for {self.uuid}, status not set.
                Error: {e}
                """
            )

    def update_asana(self) -> None:
        # Separating logic for updating Asana into a separate PR
        # UpdateAsana(self.uuid, self.token, self.status).update_process_stage()
        pass

    def send_email(self) -> None:
        """
        This assumes that either a FailureCallback has passed a SendFailureEmail
        instance or that a SendEmail subclass instance needs to be created here--
        latter case is not implemented.
        """
        if self.notification_instance:
            self.notification_instance.send_notifications()

    status_map = {}
    """
    Default behavior is to call both set_entity_api_status and update_asana.
    Add any statuses to map that require a different process.
    Example:
    {
        # "Statuses.UPLOAD_INVALID": [set_entity_api_status, update_asana, send_email],
        # "Statuses.DATASET_INVALID": [set_entity_api_status, update_asana, send_email],
        # "Statuses.DATASET_PROCESSING": [set_entity_api_status],
    }
    """

    def on_status_change(self) -> None:
        if self.status in self.status_map:
            for func in self.status_map[self.status]:
                func(self)
        else:
            self.set_entity_api_status()
            self.update_asana()
