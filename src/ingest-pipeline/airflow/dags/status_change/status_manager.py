from __future__ import annotations

import logging
from copy import deepcopy
from functools import cached_property
from typing import Any, Optional, Union

from schema_utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
)

from .data_ingest_board_manager import DataIngestBoardManager
from .email_manager import EmailManager
from .slack_manager import SlackManager
from .status_utils import (
    ENTITY_STATUS_MAP,
    EntityUpdateException,
    Statuses,
    get_run_id,
    get_submission_context,
    put_request_to_entity_api,
)

ENTITY_JSON_SCHEMA = "entity_metadata_schema.yml"  # from this repo's schemata directory


class EntityUpdater:
    def __init__(
        self,
        uuid: str,
        token: str,
        http_conn_id: str = "entity_api_connection",
        fields_to_overwrite: Optional[dict] = None,
        fields_to_append_to: Optional[dict] = None,
        delimiter: str = "|",
        reindex: bool = True,
        run_id: Optional[str] = None,
    ):
        self.uuid = uuid
        self.token = token
        self.http_conn_id = http_conn_id
        self.fields_to_overwrite = fields_to_overwrite if fields_to_overwrite else {}
        self.fields_to_append_to = fields_to_append_to if fields_to_append_to else {}
        self.delimiter = delimiter
        self.reindex = reindex
        self.run_id = get_run_id(run_id)
        self.entity_type = self.get_entity_type()
        self.fields_to_change = self.get_fields_to_change()

    @cached_property
    def entity_data(self):
        rslt = get_submission_context(self.token, self.uuid)
        return rslt

    def get_entity_type(self):
        try:
            entity_type = self.entity_data.get("entity_type")
            assert entity_type is not None
            return entity_type
        except Exception as excp:
            raise EntityUpdateException(
                f"""
                Could not find entity type for {self.uuid}.
                Error {excp}
                """
            )

    def get_fields_to_change(self) -> dict:
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
        - Calculates fields_to_change value: deduplicated collection of
            fields_to_overwrite, fields_to_append_to appended to existing fields.
        - If status key and value found: instantiates StatusChanger with update call
            and returns.
        - _check_fields ensures entity type not changed, ensures "status": None not in
            fields_to_change, and checks JSON schema of request body.
        - _set_entity_api_data sends PUT with fields_to_change payload to entity-api.
        """
        if (
            "status" in self.fields_to_change.keys()
            and self.fields_to_change["status"] is not None
        ):
            self._send_to_status_manager()
            return
        self.validate_fields_to_change()
        self.set_entity_api_data()

    def set_entity_api_data(self) -> dict:
        logging.info(
            f"""
            data:
            {self.fields_to_change}
            """
        )
        try:
            response = put_request_to_entity_api(
                self.uuid, self.token, self.fields_to_change, {"reindex": self.reindex}
            )
        except Exception as e:
            raise EntityUpdateException(
                f"""
                Encountered error with request to change fields {', '.join([key for key in self.fields_to_change])}
                for {self.uuid}, fields either not changed or not updated completely.
                Error: {e}
                """
            )
        logging.info(f"""Response: {response}""")
        return response

    def validate_fields_to_change(self):
        self._check_fields()

    def _check_fields(self):
        original_entity_type = self.entity_data.get("entity_type")
        updated_entity_data = self.entity_data.copy()
        if "status" in self.fields_to_change and self.fields_to_change["status"] is None:
            self.fields_to_change.pop("status")  # avoid setting status to None
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

    def _update_existing_values(self):
        new_field_data = {}
        for field, value in self.fields_to_append_to.items():
            existing_field_data = self.entity_data.get(field, "")
            if existing_field_data:
                new_field_data[field] = existing_field_data + f" {self.delimiter} " + value
            else:
                new_field_data[field] = value
        return new_field_data

    def _send_to_status_manager(self):
        status = self.fields_to_overwrite.pop("status", None)
        if not status:
            raise EntityUpdateException(f"Confusing call to EntityUpdater for {self.uuid}.")
        StatusChanger(
            self.uuid,
            self.token,
            http_conn_id=self.http_conn_id,
            fields_to_overwrite=self.fields_to_overwrite,
            fields_to_append_to=self.fields_to_append_to,
            delimiter=self.delimiter,
            reindex=self.reindex,
            run_id=self.run_id,
            status=status,
        ).update()

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


"""
Example usage, simple path (e.g. status string, no validation message):
    from status_manager import StatusChanger
    StatusChanger(
            "uuid_string",
            "token_string",
            status="status",
        ).update()

Example usage with optional params:
    from status_manager import StatusChanger, Statuses
    StatusChanger(
            "uuid_string",
            "token_string",
            http_conn_id="entity_api_connection",  # optional
            fields_to_overwrite={"test_field": "test"},  # optional
            fields_to_append_to={"ingest_task": "test"},  # optional
            delimiter=",",  # optional
            reindex=True,  # optional
            run_id="<airflow_run_id>",
            status=<Statuses.STATUS_ENUM>,  # or "<status>"
            message=<ErrorReport.counts>
        ).update()
"""


class StatusChanger(EntityUpdater):
    message_classes = [DataIngestBoardManager, SlackManager, EmailManager]
    same_status = False

    def __init__(
        self,
        uuid: str,
        token: str,
        http_conn_id: str = "entity_api_connection",
        fields_to_overwrite: Optional[dict] = None,
        fields_to_append_to: Optional[dict] = None,
        delimiter: str = "|",
        reindex: bool = True,
        run_id: Optional[str] = None,
        # Additional field to support privileged field "status"
        status: Optional[Union[Statuses, str]] = None,
        message=None,
        **kwargs,
    ):
        del kwargs
        super().__init__(
            uuid,
            token,
            http_conn_id,
            fields_to_overwrite,
            fields_to_append_to,
            delimiter,
            reindex,
            run_id,
        )
        self.status = self._validate_status(status)
        self.message = message

    def update(self) -> None:
        """
        This is the main method for using the StatusChanger.
        - If no status or same status and other fields_to_change, send to
            EntityUpdater; if same status continue to message step.
        - Validates fields and adds status to fields_to_change.
        - Runs EntityUpdater._set_entity_api_data() process.
        - Also instantiates messaging classes, sends updates based on class's
            is_valid_for_status value.
        """
        if self.status is None or self.same_status == True:
            if self.fields_to_change:
                self._send_to_entity_updater(
                    f"Status for {self.uuid} unchanged, sending to EntityUpdater."
                )
            else:
                logging.info(
                    f"No status update or fields to change for {self.uuid}, not making any changes in entity-api."
                )
            if not self.status:
                # Messages are tied to status, so only continue if we have one
                return
        else:
            logging.info("Updating status...")
            self.validate_fields_to_change()
            self.set_entity_api_data()
        self.call_message_managers()

    def call_message_managers(self):
        for message_type in self.message_classes:
            message_manager = message_type(
                self.status,
                self.uuid,
                self.token,
                msg=self.message,
                run_id=self.run_id,
            )
            if message_manager.is_valid_for_status:
                try:
                    message_manager.update()
                except EntityUpdateException as e:
                    # Do not blow up for known errors
                    logging.error(
                        f"Message not sent for {message_manager.message_class.name}. Error: {e}"
                    )

    def validate_fields_to_change(self):
        super().validate_fields_to_change()
        assert self.status
        self.fields_to_change["status"] = Statuses.get_status_str(self.status)

    def _validate_status(self, status: Union[Statuses, str, None]) -> Optional[Statuses]:
        current_status = self.entity_data.get("status", "").lower()
        logging.info(
            f"Current status: {current_status} ({ENTITY_STATUS_MAP[self.entity_type.lower()][current_status.lower()]})"
        )
        if not status:
            return
        # Convert to Statuses enum member if passed as str
        elif type(status) is str:
            try:
                status = ENTITY_STATUS_MAP[self.entity_type.lower()][status.lower()]
            except KeyError:
                raise EntityUpdateException(
                    f"""
                    Could not retrieve status for {self.uuid}.
                    Check that status is valid for entity type.
                    Status not changed.
                    """
                )
        assert type(status) is Statuses
        status_str = Statuses.get_status_str(status)
        logging.info(f"Pending status: {status_str} ({status})")
        # Can't set the same status over the existing status; keep status but set same_status = True.
        if status_str == current_status:
            logging.info(
                f"Status passed to StatusChanger is the same as the current status in Entity API."
            )
            self.same_status = True
        # Double-check that you don't have different values for status in other fields.
        if (extra_status := self.fields_to_change.get("status")) is not None:
            logging.info(f"Status passed in fields_to_change: {extra_status}.")
            # Assert they are the same as current status
            try:
                if isinstance(extra_status, str):
                    assert extra_status.lower() == status_str
                elif isinstance(extra_status, Statuses):
                    assert extra_status == status
            # If not, stringify for exception
            except Exception:
                if type(extra_status) is str:
                    extra_status_str = extra_status.lower()
                elif isinstance(extra_status, Statuses):
                    extra_status_str = Statuses.get_status_str(extra_status)
                else:
                    extra_status_str = str(extra_status)
                raise EntityUpdateException(
                    f"Entity {self.uuid} passed multiple statuses ({status_str} and {extra_status_str})."
                )
        return status

    def _send_to_entity_updater(self, log_msg: str):
        logging.info(log_msg)
        # Just to be sure!
        self.fields_to_overwrite.pop("status", None)
        self.fields_to_append_to.pop("status", None)
        self.fields_to_change.pop("status", None)
        # Slightly fragile, needs to keep pace with EntityUpdater.update()
        super().validate_fields_to_change()
        super().set_entity_api_data()
