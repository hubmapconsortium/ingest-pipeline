import logging
import re
from typing import Optional

from .status_utils import (
    EntityUpdateException,
    Statuses,
    get_project,
    get_submission_context,
    is_internal_error,
    log_directory_path,
    put_request_to_entity_api,
)


class DataIngestBoardManager:
    """
    Handle updating metadata fields tied to Data Ingest Board.
    """

    clear_only = [
        Statuses.DATASET_QA,
        Statuses.UPLOAD_REORGANIZED,
        Statuses.UPLOAD_VALID,
    ]
    check_primary_before_update = [
        Statuses.DATASET_QA,
        Statuses.DATASET_ERROR,
    ]
    assign_to_dp = [
        Statuses.DATASET_INVALID,
        Statuses.UPLOAD_INVALID,
    ]
    error_prefix = "Derived dataset errors"

    def __init__(
        self,
        uuid: str,
        token: str,
        status: Statuses,
        msg: str = "",
        run_id: str = "",
        handle_derived: bool = False,
        derived_dataset: Optional[dict] = None,
        *args,
        **kwargs,
    ):
        del args, kwargs
        self.uuid = uuid
        self.token = token
        self.status = status
        self.msg = str(msg) if msg else ""
        self.run_id = run_id
        self.handle_derived = handle_derived
        self.derived_dataset = derived_dataset or {}
        self.entity_data = get_submission_context(self.token, self.uuid) if self.uuid else {}
        self.entity_id_str = f"{get_project().value[0]}_id"  # "hubmap_id" or "sennet_id"
        self.update_fields = self.get_fields()
        self.is_valid_for_status = bool(self.update_fields)

    def update(self):
        if not self.update_fields or not self.uuid:
            return
        self.update_request()
        if self.handle_derived:
            self.entity_data = self.derived_dataset
            self.uuid = self.derived_dataset.get("uuid")
            if self.status is Statuses.DATASET_ERROR:
                update_data = {
                    "error_message": self.dataset_error(),
                    "assigned_to_group_name": self.assigned_to_group_name,
                }
            elif self.status is Statuses.DATASET_QA:
                update_data = {"error_message": "", "assigned_to_group_name": ""}
            else:
                return
            self.update_request(uuid=self.uuid, update_fields=update_data)

    def update_request(self, uuid=None, update_fields=None):
        uuid = uuid if uuid else self.uuid
        assert uuid
        update_fields = update_fields if update_fields else self.update_fields
        logging.info(
            f"""
            Data Ingest Board fields update:
            Entity: {uuid}
            {update_fields}
            """
        )
        try:
            response = put_request_to_entity_api(uuid, self.token, update_fields)
        except Exception as e:
            raise EntityUpdateException(
                f"""
                Encountered error with request to change fields {', '.join([key for key in update_fields])}
                for {uuid}, fields either not changed or not updated completely.
                Error: {e}
                """
            )
        logging.info(f"""Response: {response}""")
        return response

    def get_fields(self) -> dict:
        update_data = {}
        message, group_name = self.check_primary_field()
        if message or group_name:
            if message is not None:
                update_data["error_message"] = message
            if group_name is not None:
                update_data["assigned_to_group_name"] = group_name
        elif self.status in self.clear_only:
            update_data = {"error_message": "", "assigned_to_group_name": ""}
        elif func := getattr(self, self.status.value, None):
            msg = func()
            if self.msg:
                msg = f"{self.msg} | {msg}" if msg else self.msg
            update_data = {
                "error_message": msg,
                "assigned_to_group_name": self.assigned_to_group_name,
            }
        else:
            logging.info(
                f"Message method {self.status.value} not found. Not updating metadata for {self.uuid}."
            )
        return update_data

    def check_primary_field(self) -> tuple[Optional[str], Optional[str]]:
        """
        A primary dataset's `error_message` field contains info about the failures
        of any of its derived datasets. We want to:
            - case DATASET_ERROR:
                - Write the derived dataset's UUID to the primary's error_message field,
                  set assigned_to_group_name to IEC Testing Group
                - If no derived dataset was created, do not update primary
            - case DATASET_QA:
                - Remove the derived dataset's UUID from the primary's error_message field
            - Without removing message about any other erroring UUIDs or other unrelated error messages
        """
        if not self.handle_derived:
            # If a primary dataset is erroring / entering QA, we don't worry about
            # the contents of its error_message field.
            return None, None
        elif not self.status in self.check_primary_before_update:
            return None, None
        error_message = None
        group = None
        derived_entity_id = self.derived_dataset.get(self.entity_id_str, "")
        existing_error_msg = self.entity_data.get("error_message", "")
        if self.status == Statuses.DATASET_ERROR:
            error_message, group = self.check_primary_error(derived_entity_id, existing_error_msg)
        elif self.status == Statuses.DATASET_QA:
            error_message, group = self.check_primary_qa(derived_entity_id, existing_error_msg)
        return error_message, group

    def check_primary_error(
        self, derived_entity_id: str, existing_error_msg: str
    ) -> tuple[Optional[str], Optional[str]]:
        # No derived dataset ID, do not touch field
        # TODO: verify this approach
        if not derived_entity_id:
            return None, None
        # Existing error message includes 'Derived dataset errors' but does not
        # include this derived dataset ID; append ID
        if (self.error_prefix in existing_error_msg) and not (
            derived_entity_id in existing_error_msg
        ):
            error_message = f"{existing_error_msg}, {derived_entity_id}"
        # No existing error message, create with prefix and add ID
        elif not existing_error_msg:
            error_message = f"{self.error_prefix}: {derived_entity_id}"
        # Existing error message with other content; append with prefix
        else:
            error_message = f"{existing_error_msg} | {self.error_prefix}: {derived_entity_id}"
        return error_message, "IEC Testing Group"

    def check_primary_qa(
        self, derived_entity_id: str, existing_error_msg: str
    ) -> tuple[Optional[str], Optional[str]]:
        if not derived_entity_id:
            raise EntityUpdateException(
                f"Missing derived dataset ID, cannot set QA message on primary dataset {self.uuid}."
            )
        if self.error_prefix in existing_error_msg:
            split_existing_message = [
                line.strip()
                for line in re.split(": |, ", existing_error_msg)
                if line.strip() not in [derived_entity_id, self.error_prefix]
            ]
            # IDs remain after removing derived dataset ID, write to error_message
            if split_existing_message:
                msg = f"{self.error_prefix}: {', '.join(split_existing_message)}"
                group = None
                return msg, group
            # No more errors, blank error_message field
            else:
                return "", ""
        else:
            return existing_error_msg, None

    @property
    def internal_error(self) -> bool:
        # Error status or internal_error_str(s) found
        return is_internal_error(self.entity_data)

    @property
    def assigned_to_group_name(self) -> str:
        if self.internal_error:
            return "IEC Testing Group"
        elif group_name := self.entity_data.get("group_name"):
            if self.status in self.assign_to_dp:
                return group_name
        return ""

    #########################
    # Status-based messages #
    #########################

    def get_internal_error_msg(self) -> str:
        return f"Internal error. Log directory: {log_directory_path(self.run_id)}"

    def upload_error(self) -> str:
        return self.get_internal_error_msg()

    def upload_invalid(self) -> str:
        """
        Should normally return error counts.

        It is likely that anything that caused the upload to hit
        an internal error would have put the upload into an Error
        state. But, check anyway, just in case.
        """
        if self.internal_error:
            return self.get_internal_error_msg()
        elif self.msg:
            return ""
        return f"Invalid status from run {self.run_id}"

    def dataset_error(self) -> str:
        """
        Set on derived dataset. Primary error_message string constructed
        in check_primary_field().
        """
        return self.get_internal_error_msg()

    def dataset_invalid(self) -> str:
        """
        Primary datasets only.
        """
        if self.internal_error:
            return self.get_internal_error_msg()
        elif self.msg:
            return ""
        return f"Invalid status from run {self.run_id}"
