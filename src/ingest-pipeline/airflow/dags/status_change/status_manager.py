from __future__ import annotations

import json
import logging
from functools import cached_property
from typing import Literal, Optional, Union, Any

from airflow.providers.http.hooks.http import HttpHook

from .slack_formatter import format_priority_reorganized_msg
from .status_utils import (
    ENTITY_STATUS_MAP,
    EntityUpdateException,
    Statuses,
    get_submission_context,
)

from schema_utils import localized_assert_json_matches_schema as assert_json_matches_schema

from copy import deepcopy

ENTITY_JSON_SCHEMA = "entity_metadata_schema.yml"  # from this repo's schemata directory


class StatusChangeAction:
    def __init__(self, uuid: str, token: str, **kwargs):
        self.uuid = uuid
        self.token = token
        self.kwargs = kwargs

    def test(self, context: dict[str, Any]) -> bool:
        """
        The action will be run if this test returns True
        """
        return True

    def run(self, context: dict[str, Any]) -> None:
        """
        The action to be executed
        """
        pass


class CheckPriorityReorgSCA(StatusChangeAction):

    def test(self, context):
        return bool(context.get("priority_project_list"))

    def run(self, context):
        msg, channel = format_priority_reorganized_msg(self.token, self.uuid)
        if not (msg and channel):
            raise EntityUpdateException(
                f"Request to send Slack message missing message text (submitted: '{msg}')"
                f" or target channel (submitted: '{channel}')."
            )
        http_hook = HttpHook("POST", http_conn_id="ingest_api_connection")
        payload = json.dumps({"message": msg, "channel": channel})
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        response = http_hook.run("/notify", payload, headers)
        response.raise_for_status()


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
        rslt = get_submission_context(self.token, self.uuid)
        return rslt

    def get_entity_type(self):
        try:
            entity_type = self.entity_data["entity_type"]
            assert entity_type is not None
            return entity_type
        except Exception as excp:
            raise EntityUpdateException(
                f"""
                Could not find entity type for {self.uuid}.
                Error {excp}
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

    @staticmethod
    def _enums_to_lowercase(data: Any) -> Any:
        """
        Lowercase all strings which appear as dictionary values.
        This modifies the passed data in place, rather than making
        a copy!
        """
        if isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, str):
                    data[key] = val.lower()
                else:
                    data[key] = EntityUpdater._enums_to_lowercase(val)
            return data
        elif isinstance(data, list):
            return [EntityUpdater._enums_to_lowercase(val) for val in data]
        else:
            return data

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
        original_entity_type = self.entity_data.get("entity_type")
        updated_entity_data = self.entity_data.copy()
        if "status" in self.fields_to_change and self.fields_to_change["status"] is None:
            self.fields_to_change.pop("status")  # avoid setting status to None for test
        update_fields = deepcopy(self.fields_to_change)
        updated_entity_data.update(update_fields)
        updated_entity_type = updated_entity_data.get("entity_type")
        if original_entity_type != updated_entity_type:
            raise EntityUpdateException(
                "An EntityUpdater or StatusChanger cannot change the entity_type"
                f" (attempted change from {original_entity_type} to {updated_entity_type})"
            )
        try:
            assert_json_matches_schema(
                EntityUpdater._enums_to_lowercase(updated_entity_data), ENTITY_JSON_SCHEMA
            )
        except AssertionError as excp:
            raise EntityUpdateException(excp) from excp
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
            logging.error("'status' found in update fields, should use a StatusChanger.")
            raise EntityUpdateException("Status field can only be changed by a StatusChanger")

    def _update_existing_values(self):
        # TODO: guard clause, handle writing to empty field
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


class StatusChanger(EntityUpdater):
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
        super().__init__(
            uuid,
            token,
            http_conn_id,
            fields_to_overwrite,
            fields_to_append_to,
            delimiter,
            extra_options,
            verbose,
        )
        self.entity_type = entity_type if entity_type else self.get_entity_type()
        if not status:
            self.status = None
        else:
            self.status = (
                self._check_status(status)
                if isinstance(status, Statuses)
                else self._get_status(status.lower())
            )

    @property
    def status_map(self):
        """
        Add any statuses to map that require a specific set of methods in addition to
        default _set_entity_api_data.

        key: Statuses enum member
        value: list of tuples
            Each tuple represents a method to call for this status.
            tuple[0]: str(func_name)
            tuple[1]: dict containing optional kwargs
        Format: {<status>: [(<func_name>, {kwargs}), (<func_name>, {kwargs})]}
        Example: {
            Statuses.DATASET_QA: [
                (
                    "send_slack_message",
                    {"msg": f"Dataset {self.uuid} reorganized", "channel": "channel_name"},
                ),
                ("send_email", {}),
            ]
        }
        """
        return {Statuses.UPLOAD_REORGANIZED: [(CheckPriorityReorgSCA, {})]}

    def update(self) -> None:
        """
        This is the main method for using the StatusChanger.
        - If no status after instantiating (incl. if status is the same as
        existing status on entity), pass off to EntityUpdater instead so
        other fields get updated.
        - Validates fields to change, adds status that was validated in __init__.
        - Runs EntityUpdater._set_entity_api_data() process.
        - Also run methods assigned to that status in the status_map, if any.
        """
        if self.status is None and self.fields_to_change:
            self.fields_to_overwrite.pop("status", None)
            self.fields_to_append_to.pop("status", None)
            super().update()
            return
        elif self.status is None and not self.fields_to_change:
            logging.info(
                f"No status to update or fields to change for {self.uuid}, not making any changes in entity-api."
            )
            return
        self._validate_fields_to_change()
        self._set_entity_api_data()
        for action_class, args in self.status_map.get(self.status, []):
            action = action_class(self.uuid, self.token, **args)
            if action.test(self.entity_data):
                action.run(self.entity_data)

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
            logging.info("Status passed in fields_to_change.")
            assert (
                extra_status.lower() == status
            ), f"Entity {self.uuid} passed multiple statuses ({status} and {extra_status})."
        return status
