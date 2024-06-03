from __future__ import annotations

import json
import logging
from functools import cached_property
from typing import Literal, Optional, Union

from airflow.providers.http.hooks.http import HttpHook

from .status_utils import ENTITY_STATUS_MAP, Statuses, get_submission_context


class EntityUpdateException(Exception):
    pass


class EntityUpdater:
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
            raise EntityUpdateException(
                f"""
                Could not find entity type for {self.uuid}.
                Error {e}
                """
            )

    @cached_property
    def fields_to_change(self) -> dict:
        duplicates = set(self.fields_to_overwrite.keys()).intersection(
            set(self.fields_to_append_to.keys())
        )
        assert (
            not duplicates
        ), f"Field(s) {', '.join(duplicates)} cannot be both appended to and overwritten."
        return self._update_existing_values() | self.fields_to_overwrite

    def update(self):
        """
        This is the main method for using the EntityUpdater.
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
            raise EntityUpdateException(
                f"""
                Encountered error with request to change fields {', '.join([key for key in self.fields_to_change])}
                for {self.uuid}, fields either not changed or not updated completely.
                Error: {e}
                """
            )
        logging.info(f"""Response: {response.json()}""")
        return response.json()

    def _validate_fields_to_change(self):
        status_found = False
        for field in self.fields_to_change.keys():
            if field == "status":
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


class StatusChanger:
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
        status: Optional[Union[Statuses, str]] = None,
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
            raise EntityUpdateException("Must pass new status to StatusChanger.")
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
        - If no status after instantiating (incl. if status is the same as
        existing status on entity), pass off to EntityUpdater instead so
        other fields get updated.
        - Validates fields to change, adds status that was validated in __init__.
        - If a status was passed in and the status_map is populated:
            - Run methods assigned to that status in the status_map.
        - Otherwise:
            - Follows default EntityUpdater._set_entity_api_data() process.
        """
        if self.status is None and self.fields_to_change:
            logging.info(
                f"No status to update, instantiating EntityUpdater instead to update other fields: {', '.join(self.fields_to_change.keys())}"
            )
            EntityUpdater(
                self.uuid,
                self.token,
                self.http_conn_id,
                self.fields_to_overwrite,
                self.fields_to_append_to,
                self.delimiter,
                self.extra_options,
                self.verbose,
            ).update()
            return
        self._validate_fields_to_change()
        if self.status in self.status_map:
            for func in self.status_map[self.status]:
                func()
        else:
            self._set_entity_api_data()

    def _validate_fields_to_change(self):
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
            raise EntityUpdateException(
                f"""
                    Could not retrieve status for {self.uuid}.
                    Check that status is valid for entity type.
                    Status not changed.
                """
            )
        return self._check_status(entity_status)

    def _check_status(self, status: Statuses) -> Optional[Statuses]:
        if not status:
            raise EntityUpdateException(
                "No status passed to StatusChanger. To update other fields only, use EntityUpdater."
            )
        # Can't set the same status over the existing status.
        if status == self.entity_data["status"].lower():
            logging.info(
                f"Status passed to StatusChanger is the same as the current status in Entity API."
            )
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

    @cached_property
    def entity_data(self):
        return get_submission_context(self.token, self.uuid)

    def get_entity_type(self):
        try:
            entity_type = self.entity_data["entity_type"]
            assert entity_type is not None
            return entity_type
        except Exception as e:
            raise EntityUpdateException(
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
        assert not duplicates, f"""
            Field(s) {', '.join(duplicates)} cannot be both appended to and overwritten. Data sent:
            fields_to_overwrite: {self.fields_to_overwrite}
            fields_to_append_to: {self.fields_to_append_to}
            """
        return self._update_existing_values() | self.fields_to_overwrite

    def _set_entity_api_data(self) -> dict:
        endpoint = f"/entities/{self.uuid}"
        headers = {
            "authorization": "Bearer " + self.token,
            "X-SenNet-Application": "ingest-pipeline",
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
            raise EntityUpdateException(
                f"""
                Encountered error with request to change fields {', '.join([key for key in self.fields_to_change])}
                for {self.uuid}, fields (likely) not changed.
                Error: {e}
                """
            )
        logging.info(f"""Response: {response.json()}""")
        return response.json()

    def _update_existing_values(self):
        # TODO: appropriate append formatting? only certain fields allowed?
        new_field_data = {}
        for field, value in self.fields_to_append_to.items():
            existing_field_data = self.entity_data[field]
            new_field_data[field] = existing_field_data + f" {self.delimiter} " + value
        return new_field_data
