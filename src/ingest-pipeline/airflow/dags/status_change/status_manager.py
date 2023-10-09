from __future__ import annotations

import json
import logging
from enum import Enum
from functools import cached_property
from typing import Any, Dict, TypedDict, Union

from update_asana import UpdateAsana

from airflow.providers.http.hooks.http import HttpHook

from .status_utils import get_submission_context


class Statuses(str, Enum):
    # Dataset Hold and Deprecated are not currently in use but are valid for Entity API
    DATASET_DEPRECATED = "deprecated"
    DATASET_ERROR = "error"
    DATASET_HOLD = "hold"
    DATASET_INVALID = "invalid"
    DATASET_NEW = "new"
    DATASET_PROCESSING = "processing"
    DATASET_PUBLISHED = "published"
    DATASET_QA = "qa"
    UPLOAD_ERROR = "error"
    UPLOAD_INVALID = "invalid"
    UPLOAD_NEW = "new"
    UPLOAD_PROCESSING = "processing"
    UPLOAD_REORGANIZED = "reorganized"
    UPLOAD_SUBMITTED = "submitted"
    UPLOAD_VALID = "valid"


# Needed some way to disambiguate statuses shared by datasets and uploads
ENTITY_STATUS_MAP = {
    "dataset": {
        "deprecated": Statuses.DATASET_DEPRECATED,
        "error": Statuses.DATASET_ERROR,
        "hold": Statuses.DATASET_HOLD,
        "invalid": Statuses.DATASET_INVALID,
        "new": Statuses.DATASET_NEW,
        "processing": Statuses.DATASET_PROCESSING,
        "published": Statuses.DATASET_PUBLISHED,
        "qa": Statuses.DATASET_QA,
    },
    "upload": {
        "error": Statuses.UPLOAD_ERROR,
        "invalid": Statuses.UPLOAD_INVALID,
        "new": Statuses.UPLOAD_NEW,
        "processing": Statuses.UPLOAD_PROCESSING,
        "reorganized": Statuses.UPLOAD_REORGANIZED,
        "submitted": Statuses.UPLOAD_SUBMITTED,
        "valid": Statuses.UPLOAD_VALID,
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
        verbose: bool = True,
    ):
        self.uuid = uuid
        self.token = token
        self.http_conn_id = http_conn_id
        self.verbose = verbose
        self.status = (
            self.check_status(status)
            if isinstance(status, Statuses)
            else self.get_status(status, entity_type)
        )
        self.extras = (
            extras
            if extras
            else {
                "extra_fields": {},
                "extra_options": {},
            }
        )

    def get_status(self, status: str, entity_type: str | None) -> Union[Statuses, None]:
        """
        If status is passed as a string, get the entity type and match
        to correct entry in ENTITY_STATUS_MAP. Also check current status,
        because ingest-pipeline will error if you try to set the same status
        over the existing status.
        Potential TODO: could stop any operation involving "Published"
        statuses at this stage.
        """
        if entity_type is None:
            try:
                entity_type = self.entity_data["entity_type"]
                assert entity_type is not None
            except KeyError as e:
                raise StatusChangerException(
                    f"""
                    Could not reconcile entity type for {self.uuid} with status '{status}'.
                    Error {e}
                    """
                )
        try:
            entity_status = ENTITY_STATUS_MAP[entity_type.lower()][status.lower()]
        except KeyError:
            raise StatusChangerException(
                f"""
                    Could not retrieve status for {self.uuid}.
                    Check that status is valid for entity type.
                    Status not changed.
                """
            )
        return self.check_status(entity_status)

    @cached_property
    def entity_data(self):
        return get_submission_context(self.token, self.uuid)

    def check_status(self, status: Statuses) -> Union[Statuses, None]:
        if status == self.entity_data["status"].lower():
            return None
        return status

    def format_status_data(self) -> Dict[str, str | Dict]:
        data = {}
        if self.status:
            data["status"] = self.status
        # Double-check that you're not accidentally overwriting status
        if (extra_status := self.extras.get("status")) is not None and isinstance(
            extra_status, str
        ):
            assert (
                extra_status.lower() == self.status
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
                logging.info(f"Updating {self.uuid} with data {data}...")
            response = http_hook.run(
                endpoint, json.dumps(data), headers, self.extras["extra_options"]
            )
            return response.json()
        except Exception as e:
            raise StatusChangerException(
                f"""
                Encountered error with request to change status/fields
                for {self.uuid}, status not set.
                Error: {e}
                """
            )

    def update_asana(self) -> None:
        UpdateAsana(self.uuid, self.token, self.status).update_process_stage()

    def send_email(self) -> None:
        # This is underdeveloped and also requires a separate PR
        pass

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
            self.send_email()
            self.update_asana()
