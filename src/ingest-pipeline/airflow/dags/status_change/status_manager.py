from __future__ import annotations

import logging
from copy import deepcopy
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional, Union

from schema_utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
)

from .data_ingest_board_manager import DataIngestBoardManager
from .slack_manager import SlackManager
from .status_utils import (
    ENTITY_STATUS_MAP,
    EntityUpdateException,
    Statuses,
    get_submission_context,
    put_request_to_entity_api,
)

if TYPE_CHECKING:
    from submodules import ingest_validation_tools_error_report

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
    ):
        self.uuid = uuid
        self.token = token
        self.http_conn_id = http_conn_id
        self.fields_to_overwrite = fields_to_overwrite if fields_to_overwrite else {}
        self.fields_to_append_to = fields_to_append_to if fields_to_append_to else {}
        self.delimiter = delimiter
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
            status = self.fields_to_overwrite.pop("status", None)
            StatusChanger(
                self.uuid,
                self.token,
                self.http_conn_id,
                self.fields_to_overwrite,
                self.fields_to_append_to,
                self.delimiter,
                status=status,
            ).update()
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
            response = put_request_to_entity_api(self.uuid, self.token, self.fields_to_change)
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
            status=Statuses.STATUS_ENUM,  # or "<status>"
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
        # Additional field to support privileged field "status"
        status: Optional[Union[Statuses, str]] = None,
        error_report: Optional["ingest_validation_tools_error_report.ErrorReport"] = None,  # type: ignore
        **kwargs,  # Avoid blowing up if passed deprecated params
    ):
        del kwargs
        super().__init__(
            uuid,
            token,
            http_conn_id,
            fields_to_overwrite,
            fields_to_append_to,
            delimiter,
        )
        self.status = self._validate_status(status)
        self.error_report = error_report
        self.same_status = False

    @property
    def status_map(self):
        """
        Add any statuses to map that require a specific set of methods in addition to
        default _set_entity_api_data.

        key: Statuses enum member
        value: appropriate Manager class (just SlackManager currently)
        Format: {<status>: [ManagerClass]}
        Example: {
            Statuses.DATASET_QA: [SlackManager]
        }
        """

        # TODO: It may be that variation is not necessary to account for, and managers
        # can just handle any statuses passed to them that are inapplicable; re-address
        # following email integration.
        return {
            Statuses.UPLOAD_ERROR: [SlackManager, DataIngestBoardManager],
            Statuses.UPLOAD_INVALID: [SlackManager, DataIngestBoardManager],
            Statuses.UPLOAD_VALID: [SlackManager, DataIngestBoardManager],
            Statuses.UPLOAD_REORGANIZED: [SlackManager, DataIngestBoardManager],
            Statuses.DATASET_ERROR: [SlackManager, DataIngestBoardManager],
            Statuses.DATASET_INVALID: [SlackManager, DataIngestBoardManager],
            Statuses.DATASET_QA: [SlackManager, DataIngestBoardManager],
        }

    def update(self) -> None:
        """
        This is the main method for using the StatusChanger.
        - If no status (incl. if status is the same as existing status
            on entity) and other fields_to_change, instantiate
            EntityUpdater and return; otherwise log and return.
        - Adds status to fields_to_change.
        - Runs EntityUpdater._set_entity_api_data() process.
        - Also instantiates any messaging managers from status_map and calls their update methods.
        """
        if self.status is None:
            if self.fields_to_change:
                self.fields_to_overwrite.pop("status", None)
                self.fields_to_append_to.pop("status", None)
                super().update()
            else:
                logging.info(
                    f"No status to update or fields to change for {self.uuid}, not making any changes in entity-api."
                )
            return
        elif self.same_status == True:
            logging.info(
                f"Same status passed, no fields to change for {self.uuid}, skipping entity-api update; sending relevant notifications."
            )
            if self.fields_to_change:
                self.fields_to_overwrite.pop("status", None)
                self.fields_to_append_to.pop("status", None)
                super().update()
        else:
            self.validate_fields_to_change()
            self.set_entity_api_data()
        for message_method in self.status_map.get(self.status, []):
            message_method(self.status, self.uuid, self.token, self.error_report).update()

    def validate_fields_to_change(self):
        super().validate_fields_to_change()
        assert self.status
        self.fields_to_change["status"] = self.status.value

    def _validate_status(self, status: Union[Statuses, str, None]) -> Optional[Statuses]:
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
        # Can't set the same status over the existing status; keep status but set same_status = True.
        if status.value == self.entity_data["status"].lower():
            logging.info(
                f"Status passed to StatusChanger is the same as the current status in Entity API."
            )
            self.same_status = True
        # Double-check that you don't have different values for status in other fields.
        elif (extra_status := self.fields_to_change.get("status")) is not None and isinstance(
            extra_status, str
        ):
            logging.info("Status passed in fields_to_change.")
            assert (
                extra_status.lower() == status
            ), f"Entity {self.uuid} passed multiple statuses ({status} and {extra_status})."
        return status
