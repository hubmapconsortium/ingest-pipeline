from __future__ import annotations

import logging
from copy import deepcopy
from functools import cached_property

from schema_utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
)

from .data_ingest_board_manager import DataIngestBoardManager
from .email_manager import EmailManager
from .slack_manager import SlackManager
from .statistics_manager import StatisticsManager
from .status_utils import (
    ENTITY_STATUS_MAP,
    EntityUpdateException,
    MessageManager,
    Statuses,
    enums_to_lowercase,
    format_multiline,
    get_status_enum,
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
        fields_to_overwrite: dict | None = None,
        fields_to_append_to: dict | None = None,
        delimiter: str = "|",
        reindex: bool = True,
    ):
        """Update metadata fields for a given entity.
        Basic usage, overwrite field:
            EntityUpdater(
                "uuid_string",
                "token_string",
                fields_to_overwrite={
                    "validation_message": "Errors found!"
                    }
                ).update()
        uuid -- uuid of entity to update
        token -- Globus token
        http_conn_id -- Entity API http connection for Airflow
        fields_to_overwrite -- dict of {fieldname: new_value},
            where new_value will replace the existing field value
        fields_to_append_to -- dict of {fieldname: new_value},
            where new_value will be appended to existing field value
        delimiter -- choose how to separate existing & new field
            values when appending
        reindex -- whether to specify reindex in request to Entity API
        """
        self.uuid = uuid
        self.token = token
        self.http_conn_id = http_conn_id
        self.fields_to_overwrite = fields_to_overwrite if fields_to_overwrite else {}
        self.fields_to_append_to = fields_to_append_to if fields_to_append_to else {}
        self.delimiter = delimiter
        self.reindex = reindex
        self.entity_type = self.get_entity_type()
        self.fields_to_change = self.get_fields_to_change()

    @cached_property
    def entity_data(self) -> dict:
        rslt = get_submission_context(self.token, self.uuid)
        return rslt

    def get_entity_type(self) -> str:
        try:
            entity_type = self.entity_data.get("entity_type")
            assert entity_type is not None
            return entity_type
        except Exception as excp:
            raise EntityUpdateException(
                format_multiline(
                    f"""
                Could not find entity type for {self.uuid}.
                Error {excp}
                """
                )
            )

    ##########
    # Public #
    ##########

    def update(self):
        """
        - If status found in fields_to_change: send to StatusChanger then return.
        - _check_fields ensures entity type not changed, ensures {"status": None} not
            in fields_to_change, and checks JSON schema of request body.
        - _set_entity_api_data sends PUT with fields_to_change payload to entity-api.
        """
        if self.fields_to_change.get("status"):
            self._send_to_status_manager()
            return
        self.validate_fields_to_change()
        self.set_entity_api_data()

    ##########
    # Fields #
    ##########

    def get_fields_to_change(self) -> dict:
        """
        - Ensure that there are no conflicting instructions (append and overwrite)
        for any fields.
        - Append values from fields_to_append_to to existing metadata values.
        - Combine appended fields and fields_to_overwrite.
        """
        duplicates = set(self.fields_to_overwrite.keys()).intersection(
            set(self.fields_to_append_to.keys())
        )
        assert (
            not duplicates
        ), f"Field(s) {', '.join(duplicates)} cannot be both appended to and overwritten."
        return self._update_existing_values() | self.fields_to_overwrite

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
            assert_json_matches_schema(enums_to_lowercase(updated_entity_data), ENTITY_JSON_SCHEMA)
        except AssertionError as excp:
            raise EntityUpdateException(excp) from excp

    def _update_existing_values(self) -> dict:
        new_field_data = {}
        for field, value in self.fields_to_append_to.items():
            existing_field_data = self.entity_data.get(field, "")
            if existing_field_data:
                new_field_data[field] = existing_field_data + f" {self.delimiter} " + value
            else:
                new_field_data[field] = value
        return new_field_data

    ##########
    # Update #
    ##########

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
                format_multiline(
                    f"""
                Encountered error with request to change fields {', '.join([key for key in self.fields_to_change])}
                for {self.uuid}, fields either not changed or not updated completely.
                Error: {e}
                """
                )
            )
        logging.info(f"""Response: {response}""")
        return response

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
            status=status,
        ).update()


class StatusChanger(EntityUpdater):
    message_classes = [DataIngestBoardManager, SlackManager, EmailManager, StatisticsManager]
    same_status = False

    def __init__(
        self,
        uuid: str,
        token: str,
        http_conn_id: str = "entity_api_connection",
        fields_to_overwrite: dict | None = None,
        fields_to_append_to: dict | None = None,
        delimiter: str = "|",
        reindex: bool = True,
        # Additional field to support privileged field "status"
        status: Statuses | str | None = None,
        # Additional field to pass data to messaging classes
        messages: dict | None = None,
        message_classes: list[str | type[MessageManager]] = [],
        **kwargs,
    ):
        """Specialized subclass to handle status updates and call appropriate
        message managers.

        Basic usage, just status string (no additional fields or messages):
            from status_manager import StatusChanger
            StatusChanger(
                    "uuid_string",
                    "token_string",
                    status="status",
                ).update()
        status -- Statuses enum or string version of new status
        messages -- dict for parsing by MessageManager classes,
            e.g. {
                    "error_dict": {<content>},
                    "error_counts": <counts_str>,
                    "run_id": <run_id>
                    "pipeline_name": <pipeline_name>
                }

        """
        del kwargs
        super().__init__(
            uuid,
            token,
            http_conn_id,
            fields_to_overwrite,
            fields_to_append_to,
            delimiter,
            reindex,
        )
        self.status = self._validate_status(status)
        self.messages = messages
        if message_classes:
            self.message_classes = message_classes

    ##########
    # Public #
    ##########

    def update(self) -> None:
        """
        - If no status or if same_status and fields_to_change, send to
            EntityUpdater.
            - If no status: return (nothing left to do).
            - If same_status: continue to call_message_managers.
        - Validates fields with parent method and adds status to fields_to_change.
        - Runs parent _set_entity_api_data() process.
        - Instantiates messaging classes, sends updates based on message class's
            is_valid_for_status value.
        """
        if self.status is None or self.same_status:
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
        call_message_managers(
            self.status, self.uuid, self.token, self.messages, self.message_classes
        )

    ##########
    # Fields #
    ##########

    def validate_fields_to_change(self):
        super().validate_fields_to_change()
        assert self.status
        self.fields_to_change["status"] = self.status.status_str

    def _validate_status(self, status: Statuses | str | None) -> Statuses | None:
        current_status = self.entity_data.get("status", "").lower()
        logging.info(
            f"Current status: {current_status} ({ENTITY_STATUS_MAP[self.entity_type.lower()][current_status.lower()]})"
        )
        if not status:
            return
        # Convert to Statuses enum member if passed as str
        elif type(status) is str:
            status = get_status_enum(self.entity_type, status, self.uuid)
        assert type(status) is Statuses
        logging.info(f"Pending status: {status.status_str} ({status})")
        # Can't set the same status over the existing status; keep status but set same_status = True.
        if status.status_str == current_status:
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
                    assert extra_status.lower() == status.status_str
                elif isinstance(extra_status, Statuses):
                    assert extra_status == status
            # If not, stringify for exception
            except Exception:
                if type(extra_status) is str:
                    extra_status_str = extra_status.lower()
                elif type(extra_status) is Statuses:
                    extra_status_str = extra_status.status_str
                else:
                    extra_status_str = str(extra_status)
                raise EntityUpdateException(
                    f"Entity {self.uuid} passed multiple statuses ({status.status_str} and {extra_status_str})."
                )
        return status

    ##########
    # Update #
    ##########

    def _send_to_entity_updater(self, log_msg: str):
        logging.info(log_msg)
        # Just to be sure!
        self.fields_to_overwrite.pop("status", None)
        self.fields_to_append_to.pop("status", None)
        self.fields_to_change.pop("status", None)
        # Slightly fragile, needs to keep pace with EntityUpdater.update()
        super().validate_fields_to_change()
        super().set_entity_api_data()


message_class_map = {
    "DataIngestBoardManager": DataIngestBoardManager,
    "SlackManager": SlackManager,
    "EmailManager": EmailManager,
    "StatisticsManager": StatisticsManager,
}


def get_message_manager_class(message_type: str | type[MessageManager]) -> type[MessageManager]:
    if isinstance(message_type, str):
        if message_class := message_class_map.get(message_type):
            return message_class
    elif type(message_type) is type(MessageManager):
        return message_type
    raise Exception(f"MessageManager class {message_type} not found. Skipping.")


def call_message_managers(
    status: Statuses | str,
    uuid: str,
    token: str,
    messages: dict | None = None,
    message_classes: list[str | type[MessageManager]] = [],
):
    if not message_classes:
        validated_message_classes = list(message_class_map.values())
    else:
        validated_message_classes = []
        for msg_class in message_classes:
            try:
                validated_message_classes.append(get_message_manager_class(msg_class))
            except Exception as e:
                logging.error(e)
    for message_type in validated_message_classes:
        message_manager = message_type(
            status,
            uuid,
            token,
            messages=messages,
        )
        if message_manager.is_valid_for_status:
            try:
                message_manager.update()
            except EntityUpdateException as e:
                # Do not blow up for known errors
                logging.error(
                    f"Message not sent from manager class {type(message_manager).__name__}. Error: {e}"
                )
        else:
            logging.info(
                f"Message manager class {type(message_manager).__name__} not valid for status {status}, skipping."
            )
