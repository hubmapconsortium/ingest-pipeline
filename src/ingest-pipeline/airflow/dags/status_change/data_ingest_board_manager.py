import logging
from typing import Optional

from utils import get_submission_context

from .status_utils import (
    EntityUpdateException,
    MessageManager,
    Statuses,
    get_primary_dataset,
    put_request_to_entity_api,
)


class DataIngestBoardManager(MessageManager):
    """
    Handle updating metadata fields tied to Data Ingest Board.
    """

    clear_only = [
        "upload_reorganized",
        "upload_valid",
        "dataset_qa",
    ]

    assign_to_dp = [Statuses.DATASET_INVALID, Statuses.UPLOAD_INVALID]

    def __init__(
        self,
        status: Statuses,
        uuid: str,
        token: str,
        messages: Optional[dict] = None,
        *args,
        **kwargs,
    ):
        super().__init__(status, uuid, token, messages, *args, **kwargs)
        self.update_fields = self.get_fields()

    @property
    def is_valid_for_status(self):
        return bool(self.update_fields)

    def update(self):
        if not self.update_fields:
            return
        logging.info(
            f"""
            Data Ingest Board fields update:
            {self.update_fields}
            """
        )
        try:
            response = put_request_to_entity_api(self.uuid, self.token, self.update_fields)
        except Exception as e:
            raise EntityUpdateException(
                f"""
                Encountered error with request to change fields {', '.join([key for key in self.update_fields])}
                for {self.uuid}, fields either not changed or not updated completely.
                Error: {e}
                """
            )
        logging.info(f"""Response: {response}""")
        return response

    ################
    # Prepare data #
    ################

    def get_fields(self) -> Optional[dict]:
        msg_type = self.status.value
        # Check if status just needs to be cleared first
        if clear_msg := self.get_clear_message(msg_type):
            return clear_msg
        # Find matching function (format <entity_type>_<status>)
        func = getattr(self, msg_type, None)
        if not func:
            logging.info(
                f"Message method {msg_type} not found. Not updating metadata for {self.uuid}."
            )
            return
        update_data = func()
        # Add or clear assigned_to_group_name
        if self.assigned_to_group_name:
            update_data["assigned_to_group_name"] = self.assigned_to_group_name
        else:
            update_data["assigned_to_group_name"] = ""
        return update_data

    def get_clear_message(self, msg_type: str) -> Optional[dict]:
        """
        Clear error messages following success.
        If this is a derived dataset:
            - clear derived dataset error_message
            - locate primary dataset
            - make sure erroring derived dataset uuid is part of primary error_message
            - reset uuid to primary
            - clear primary error message
        Else:
            - clear error message
        """
        clear_message = {"error_message": ""}
        if msg_type in self.clear_only:
            if self.derived:
                # Clear message on derived dataset
                self.update_fields = clear_message
                self.update()
                # Try to clear message on primary
                if not (primary_uuid := self.overwrite_primary()):
                    return
                self.uuid = primary_uuid
            return {"error_message": ""}

    @property
    def assigned_to_group_name(self) -> Optional[str]:
        if self.is_internal_error:
            return "IEC Testing Group"
        elif group_name := self.entity_data.get("group_name"):
            if self.status in self.assign_to_dp:
                return group_name

    #########################
    # Status-based messages #
    #########################

    def get_internal_error_msg(self) -> str:
        prefix = f"Internal error. Log directory: {self.log_directory_path}"
        if self.error_counts:
            error = f"{prefix} | {self.error_counts}"
        else:
            error = prefix
        return error

    def upload_error(self):
        return {"error_message": self.get_internal_error_msg()}

    def upload_invalid(self):
        """
        Should normally return error counts.

        It is likely that anything that caused the upload to hit
        an internal error would have put the upload into an Error
        state. But, check anyway, just in case.
        """
        if self.is_internal_error:
            error = self.get_internal_error_msg()
        else:
            error = (
                self.error_counts
                if self.error_counts
                else f"Invalid status from run {self.run_id}"
            )
        return {"error_message": error}

    def dataset_error(self):
        """
        If this is a derived dataset:
            - locate primary dataset
            - reset uuid to primary
            - return error message
            * No use setting error on derived, does not appear on Data Ingest Board.
        If this is a primary dataset that failed during processing:
            - report as pipeline failure
        Else:
            - return internal error message
        """
        if self.derived:
            if primary_uuid := self.overwrite_primary(check_existing=False):
                derived_uuid = self.uuid
                self.uuid = primary_uuid
                return {"error_message": f"Derived dataset {derived_uuid} is in Error state."}
        elif self.is_primary:
            return {"error_message": f"Pipeline failure: {self.processing_pipeline}."}
        else:
            return {"error_message": self.get_internal_error_msg()}

    def dataset_invalid(self):
        if self.is_internal_error:
            error = self.get_internal_error_msg()
        else:
            error = (
                self.error_counts
                if self.error_counts
                else f"Invalid status from run {self.run_id}"
            )
        return {"error_message": error}

    #########
    # Utils #
    #########

    def overwrite_primary(self, check_existing: bool = True) -> str | None:
        assert self.derived
        # Derived datasets need to write to primary dataset error_message
        derived_uuid = self.uuid
        primary_uuid = get_primary_dataset(self.entity_data, self.token)
        # Make sure we have primary uuid
        if not primary_uuid:
            raise EntityUpdateException(
                f"Primary dataset uuid not found for derived dataset {derived_uuid}, will not clear error_message."
            )
        primary_data = get_submission_context(self.token, primary_uuid)
        if check_existing:
            # Make sure primary's error_message is appropriate to clear
            if not derived_uuid in primary_data.get("error_message", ""):
                return
        logging.info(f"Existing error message for primary dataset {self.uuid}:")
        logging.info(primary_data.get("error_message"))
        return primary_uuid
