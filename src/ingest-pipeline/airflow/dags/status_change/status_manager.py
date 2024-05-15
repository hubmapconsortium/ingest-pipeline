from __future__ import annotations

import json
import logging
from enum import Enum
from functools import cached_property
from typing import Any, Literal, Optional, TypedDict

from status_utils import get_submission_context

from airflow.providers.http.hooks.http import HttpHook


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
    DATASET_SUBMITTED = "submitted"
    PUBLICATION_ERROR = "error"
    PUBLICATION_HOLD = "hold"
    PUBLICATION_INVALID = "invalid"
    PUBLICATION_NEW = "new"
    PUBLICATION_PROCESSING = "processing"
    PUBLICATION_PUBLISHED = "published"
    PUBLICATION_QA = "qa"
    PUBLICATION_SUBMITTED = "submitted"
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
        "submitted": Statuses.DATASET_SUBMITTED,
    },
    "publication": {
        "error": Statuses.PUBLICATION_ERROR,
        "hold": Statuses.PUBLICATION_HOLD,
        "invalid": Statuses.PUBLICATION_INVALID,
        "new": Statuses.PUBLICATION_NEW,
        "processing": Statuses.PUBLICATION_PROCESSING,
        "published": Statuses.PUBLICATION_PUBLISHED,
        "qa": Statuses.PUBLICATION_QA,
        "submitted": Statuses.PUBLICATION_SUBMITTED,
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


class EntityApiUpdater:
    def __init__(
        self,
        uuid: str,
        token: str,
        http_conn_id: str = "entity_api_connection",
        fields_to_overwrite: Optional[dict] = None,
        fields_to_append_to: Optional[dict] = None,
        delimiter: str = "|",
        extra_options: Optional[dict] = None,
        verbose: bool = True,
    ):
        self.uuid = uuid
        self.token = token
        self.http_conn_id = http_conn_id
        self.fields_to_overwrite = fields_to_overwrite if fields_to_overwrite else {}
        self.fields_to_append_to = fields_to_append_to if fields_to_append_to else {}
        self.delimiter = delimiter
        self.extra_options = extra_options if extra_options else {}
        self.verbose = verbose
        self.entity_type = self.get_entity_type()

    @cached_property
    def entity_data(self):
        return get_submission_context(self.token, self.uuid)

    def get_entity_type(self):
        try:
            entity_type = self.entity_data["entity_type"]
            assert entity_type is not None
            return entity_type
        except Exception as e:
            raise StatusChangerException(
                f"""
                Could not find entity type for {self.uuid}.
                Error {e}
                """
            )

    @cached_property
    def fields_to_change(self) -> dict:
        # TODO: check directionality on this
        duplicates = set(self.fields_to_overwrite.keys()).intersection(
            set(self.fields_to_append_to.keys())
        )
        assert (
            not duplicates
        ), f"Field(s) {', '.join(duplicates)} cannot be both appended to and overwritten."
        return self._update_existing_values() | self.fields_to_overwrite

    def update(self):
        """
        This is the main method for using the EntityApiUpdater.
        - Appends values of fields_to_append_to to existing entity-api fields.
        - Compiles fields to change: fields_to_overwrite + appended fields,
          ensuring there are no duplicates.
        - Validates existence of fields_to_change against fields in entity-api data.
        - If send_to_status_changer and "status" is found in fields_to_change,
          creates a StatusChanger instance and passes validated data.
        - Otherwise, makes a PUT request with fields_to_change payload to entity-api.
        - Returns response.json() or raises Exception.
        """
        self._validate_fields_to_change()
        self._set_entity_api_data()

    def _set_entity_api_data(self) -> dict:
        endpoint = f"/entities/{self.uuid}"
        headers = {
            "authorization": "Bearer " + self.token,
            "X-Hubmap-Application": "ingest-pipeline",
            "content-type": "application/json",
        }
        http_hook = HttpHook("PUT", http_conn_id=self.http_conn_id)
        if self.extra_options.get("check_response") is None:
            self.extra_options.update({"check_response": True})
        logging.info(
            f"""
            data:
            {self.fields_to_change}
            """
        )
        if self.verbose:
            logging.info(f"Updating {self.uuid} with data {self.fields_to_change}...")
        try:
            response = http_hook.run(
                endpoint, json.dumps(self.fields_to_change), headers, self.extra_options
            )
        except Exception as e:
            raise StatusChangerException(
                f"""
                Encountered error with request to change fields {', '.join([key for key in self.fields_to_change])}
                for {self.uuid}, fields (likely) not changed.
                Error: {e}
                """
            )
        logging.info(f"""Response: {response.json()}""")
        return response.json()

    def _validate_fields_to_change(self):
        # TODO: this does basic key checking but should it also try to check value type?
        status_found = False
        for field in self.fields_to_change.keys():
            if not field in self.entity_data.keys():
                raise Exception(f"Field {field} is invalid for entity type {self.entity_type}.")
            elif field == "status":
                status_found = True
        if status_found:
            logging.info("'status' found in update fields, sending to StatusChanger.")
            StatusChanger(
                self.uuid,
                self.token,
                self.http_conn_id,
                self.fields_to_overwrite,
                self.fields_to_append_to,
                self.delimiter,
                self.extra_options,
                self.verbose,
                self.fields_to_change["status"],
                self.entity_type,
            ).update()

    def _update_existing_values(self):
        # TODO: appropriate append formatting? only certain fields allowed?
        new_field_data = {}
        for field, value in self.fields_to_append_to.items():
            existing_field_data = self.entity_data[field]
            new_field_data[field] = existing_field_data + f" {self.delimiter} " + value
        return new_field_data


"""
Example usage, simple path (e.g. status string, no validation message):
    from status_manager import StatusChanger
    StatusChanger(
            "uuid_string",
            "token_string",
            status="status",
        ).update()

Example usage with some optional params:
    from status_manager import StatusChanger, Statuses
    StatusChanger(
            "uuid_string",
            "token_string",
            http_conn_id="entity_api_connection",  # optional
            fields_to_overwrite={"test_field": "test"},  # optional
            fields_to_append_to={"ingest_task": "test"},  # optional
            delimiter=",",  # optional
            extra_options={},  # optional
            verbose=True,  # optional
            status=Statuses.STATUS_ENUM,  # or "<status>"
            entity_type="Dataset"|"Upload"|"Publication"  # optional
        ).update()
"""


class StatusChanger(EntityApiUpdater):
    def __init__(
        self,
        uuid: str,
        token: str,
        http_conn_id: str = "entity_api_connection",
        fields_to_overwrite: Optional[dict] = None,
        fields_to_append_to: Optional[dict] = None,
        delimiter: str = "|",
        extra_options: Optional[dict] = None,
        verbose: bool = True,
        # Additional fields added to support privileged field "status"
        status: Optional[Statuses | str] = None,
        entity_type: Optional[Literal["Dataset", "Upload", "Publication"]] = None,
    ):
        self.uuid = uuid
        self.token = token
        self.http_conn_id = http_conn_id
        self.fields_to_overwrite = fields_to_overwrite if fields_to_overwrite else {}
        self.fields_to_append_to = fields_to_append_to if fields_to_append_to else {}
        self.delimiter = delimiter
        self.extra_options = extra_options if extra_options else {}
        self.verbose = verbose
        if not status:
            raise Exception("Must pass new status to StatusChanger.")
        self.entity_type = entity_type if entity_type else self.get_entity_type()
        self.status = (
            self._check_status(status)
            if isinstance(status, Statuses)
            else self._get_status(status.lower())
        )

    status_map = {}
    """
    Add any statuses to map that require a specific set of methods.
    Example:
    {
        # "Statuses.UPLOAD_INVALID": [_set_entity_api_status, send_email],
        # "Statuses.DATASET_INVALID": [_set_entity_api_status, send_email],
        # "Statuses.DATASET_PROCESSING": [_set_entity_api_status],
    }
    """

    def update(self) -> None:
        """
        This is the main method for using the StatusChanger.
        Validates fields to change, adds status that was validated in __init__.
        If a status was passed in and the status_map is populated:
            - Run methods assigned to that status in the status_map.
        Otherwise:
            - Follows default EntityApiUpdater._set_entity_api_data() process.
        """
        self._validate_fields_to_change()
        if self.status in self.status_map:
            for func in self.status_map[self.status]:
                func()
        else:
            self._set_entity_api_data()

    def _validate_fields_to_change(self):
        # TODO: this does basic key checking but should it also try to check value type?
        for field in self.fields_to_change.keys():
            if not field in self.entity_data.keys():
                raise Exception(f"Field {field} is invalid for entity type {self.entity_type}.")
        self.fields_to_change["status"] = self.status

    def _get_status(self, status: str) -> Optional[Statuses]:
        """
        If status is passed as a string, get the entity type and match
        to correct entry in ENTITY_STATUS_MAP.
        Potential TODO: could stop any operation involving "Published"
        statuses at this stage.
        """
        try:
            entity_status = ENTITY_STATUS_MAP[self.entity_type.lower()][status]
        except KeyError:
            raise StatusChangerException(
                f"""
                    Could not retrieve status for {self.uuid}.
                    Check that status is valid for entity type.
                    Status not changed.
                """
            )
        return self._check_status(entity_status)

    def _check_status(self, status: Statuses) -> Optional[Statuses]:
        if not status:
            raise Exception(
                f"No status passed to StatusChanger. To update other fields only, use EntityApiUpdater."
            )
        # Can't set the same status over the existing status.
        if status == self.entity_data["status"].lower():
            return None
        # Double-check that you don't have different values for status in other fields.
        if (extra_status := self.fields_to_change.get("status")) is not None and isinstance(
            extra_status, str
        ):
            assert (
                extra_status.lower() == self.status
            ), f"Entity {self.uuid} passed multiple statuses ({self.status} and {extra_status})."
        return status

    def send_email(self) -> None:
        # This is underdeveloped and also requires a separate PR
        pass
