from __future__ import annotations

import json
import logging
from enum import Enum
from typing import Any, TypedDict

from airflow.providers.http.hooks.http import HttpHook

"""
TODO:
    - Email capability?
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


# Needed some way to disambiguate statuses shared by datasets and uploads
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
Example usage, default path:
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
        logging.info(f"COMPILED DATA: {data}")
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
            {data}
            """
        )
        try:
            if self.verbose and data.get("status") is not None:
                logging.info(f"Setting status to {data['status']}...")
            response = http_hook.run(
                endpoint, json.dumps(data), headers, self.extras["extra_options"]
            )
            # if self.verbose:
            #     logging.info(
            #         f"""
            #             Response:
            #             {json.dumps(response.json(), indent=6)}
            #         """
            #     )
            return response.json()
        except Exception as e:
            raise StatusChangerException(
                f"Encountered error with request to change status for {self.uuid}, status not set. Error: {e}"
            )

    def update_asana(self) -> None:
        # Separating logic for updating Asana into a separate PR
        # UpdateAsana(self.uuid, self.token, self.status).update_process_stage()
        pass

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
