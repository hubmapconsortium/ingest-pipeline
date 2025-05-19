from __future__ import annotations

import json
import logging
from functools import cached_property
from typing import Optional, Union

from airflow.configuration import conf as airflow_conf
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
                self.fields_to_change["status"],
                self.http_conn_id,
                self.fields_to_overwrite,
                self.fields_to_append_to,
                self.delimiter,
                self.extra_options,
                self.verbose,
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
            status=Statuses.STATUS_ENUM,  # or "<status>"
            http_conn_id="entity_api_connection",  # optional
            fields_to_overwrite={"test_field": "test"},  # optional
            fields_to_append_to={"ingest_task": "test"},  # optional
            delimiter=",",  # optional
            extra_options={},  # optional
            verbose=True,  # optional
        ).update()
"""


class StatusChanger(EntityUpdater):
    """
    Add any statuses to map that require a specific set of methods in addition to
    default _set_entity_api_data.
    """

    status_map = {Statuses.UPLOAD_REORGANIZED: ["_check_priority_reorganized"]}

    def __init__(
        self,
        uuid: str,
        token: str,
        status: Union[Statuses, str],
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
        self.status = self._check_status(status)

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
        self._validate_fields_to_change()
        self._set_entity_api_data()
        for func in self.status_map.get(self.status, []):
            getattr(self, func)()

    def _validate_fields_to_change(self):
        self.fields_to_change["status"] = self.status

    def _get_status(self, status: str) -> Optional[Statuses]:
        """
        If status is passed as a string, get the entity type and match
        to correct entry in ENTITY_STATUS_MAP.
        Potential TODO: could stop any operation involving "Published"
        statuses at this stage.
        """
        entity_status = ENTITY_STATUS_MAP.get(self.entity_type.lower(), {}).get(status.lower())
        return entity_status

    def _check_status(self, status: Union[Statuses, str, None]) -> Statuses:
        # Convert str to Status if possible, else status = None
        if type(status) is str:
            status = self._get_status(status)
        # Can't set the same status over the existing status, if match then status = None
        if isinstance(status, Statuses) and status.value == self.entity_data["status"].lower():
            logging.info(
                f"Status passed to StatusChanger is the same as the current status in Entity API."
            )
            status = None
        # Handle status = None
        if not status:
            if self.fields_to_change:
                self.fields_to_overwrite.pop("status", None)
                self.fields_to_append_to.pop("status", None)
                logging.info(
                    f"No status to update, instantiating EntityUpdater instead to update other fields: {', '.join(self.fields_to_change.keys())}"
                )
                super().update()
            else:
                raise EntityUpdateException(
                    "No status passed to StatusChanger and no additional fields provided to change."
                )
        # Should definitely have Statuses type at this point
        assert isinstance(
            status, Statuses
        ), f"Error while checking status '{status}' of type {type(status)}."
        # Double-check that you don't have different values for status in other fields.
        if (extra_status := self.fields_to_change.get("status")) is not None and isinstance(
            extra_status, str
        ):
            logging.info("Status passed in fields_to_change.")
            assert (
                extra_status.lower() == status.value
            ), f"Entity {self.uuid} passed multiple statuses ({status} and {extra_status})."
        return status

    def send_email(self) -> None:
        # This is underdeveloped and also requires a separate PR
        pass

    @cached_property
    def entity_data(self):
        return get_submission_context(self.token, self.uuid)

    def get_entity_type(self) -> str:
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
                for {self.uuid}, fields (likely) not changed.
                Error: {e}
                """
            )
        logging.info(f"""Response: {response.json()}""")
        return response.json()

    def _update_existing_values(self) -> dict:
        # TODO: appropriate append formatting? only certain fields allowed?
        new_field_data = {}
        for field, value in self.fields_to_append_to.items():
            existing_field_data = self.entity_data[field]
            new_field_data[field] = existing_field_data + f" {self.delimiter} " + value
        return new_field_data

    def send_slack_message(self, msg: str, channel: str) -> dict:
        if not (msg and channel):
            raise EntityUpdateException(
                f"Request to send Slack message missing message text (submitted: '{msg}') or target channel (submitted: '{channel}')."
            )
        http_hook = HttpHook("PUT", http_conn_id="ingest_api_connection")
        payload = json.dumps({"message": msg, "channel": channel})
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        response = http_hook.run("/notify", payload, headers)
        response.raise_for_status()
        return response.json()

    def _check_priority_reorganized(self) -> None:
        priority_reorganized_channel = (
            airflow_conf.as_dict().get("slack_channels", {}).get("PRIORITY_UPLOAD_REORGANIZED", "")
        )
        if project_list := self.entity_data.get("priority_project_list"):
            self.send_slack_message(
                f"Priority upload {self.entity_data.get('uuid')} reorganized. Priority upload categories: {', '.join(cat for cat in project_list)}.",
                str(
                    priority_reorganized_channel
                ),  # type checker thinks this could also be a tuple
            )
