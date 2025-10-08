import logging
from typing import Optional

from .status_utils import (  # get_primary_dataset,
    EntityUpdateException,
    Statuses,
    get_submission_context,
    is_internal_error,
    put_request_to_entity_api,
)


class DataIngestBoardManager:
    # TODO: derived datasets are not created until pipeline steps succeed;
    # therefore there is no status change to indicate failed pipeline run.
    # Pipelines hit set_dataset_error but do not set error on primary.
    # Perhaps add EntityUpdater call *somewhere* in set_dataset_error?
    """
    Handle updating metadata fields tied to Data Ingest Board.
    """

    clear_only = [
        "upload_reorganized",
        "upload_valid",
        "dataset_qa",
    ]

    # assign_to_dp = [Statuses.DATASET_INVALID, Statuses.UPLOAD_INVALID]  # ?? +QA?
    assign_to_dp = []  # leaving blank, need to define with curators

    def __init__(
        self,
        status: Statuses,
        uuid: str,
        token: str,
        msg: str = "",
        run_id: str = "",
        *args,
        **kwargs,
    ):
        del args, kwargs
        self.uuid = uuid
        self.token = token
        self.status = status
        self.msg = str(msg) if msg else ""
        self.run_id = run_id
        self.entity_data = get_submission_context(self.token, self.uuid)
        self.update_fields = self.get_fields()
        self.is_valid_for_status = bool(self.update_fields)

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

    def get_fields(self) -> Optional[dict]:
        entity = self.entity_data.get("entity_type", "").lower()
        msg_type = f"{entity}_{Statuses.get_status_str(self.status)}"
        if clear_msg := self.get_clear_message(msg_type, entity):
            return clear_msg
        func = getattr(self, msg_type, None)
        if not func:
            logging.info(
                f"Message method {msg_type} not found. Not updating metadata for {self.uuid}."
            )
            return
        update_data = func()
        if self.assigned_to_group_name:
            update_data["assigned_to_group_name"] = self.assigned_to_group_name
        return update_data

    def get_clear_message(self, msg_type: str, entity: str) -> Optional[dict]:
        """
        Clear error messages following success.
        """
        if msg_type in self.clear_only:
            # if entity == "dataset":
            #     # Derived datasets need to write to primary dataset error_message;
            #     # set self.uuid to primary if uuid passed in is for a derived dataset.
            #     self.check_is_derived
            return {"error_message": ""}

    @property
    def internal_error(self) -> bool:
        # Error status or internal_error_str(s) found
        return is_internal_error(self.entity_data)

    @property
    def assigned_to_group_name(self) -> Optional[str]:
        if self.internal_error:
            return "IEC Testing Group"
        elif group_name := self.entity_data.get("group_name"):
            if self.status in self.assign_to_dp:
                return group_name

    @property
    def log_directory_path(self) -> str:
        from utils import get_tmp_dir_path

        return str(get_tmp_dir_path(self.run_id))

    ########################
    #
    # Status-based messages
    #
    ########################

    def get_internal_error_prefix(self) -> str:
        prefix = f"Internal error. Log directory: {self.log_directory_path}"
        if self.msg:
            error = f"{prefix} | {self.msg}"
        else:
            error = prefix
        return error

    def upload_error(self):
        return {"error_message": self.get_internal_error_prefix()}

    def upload_invalid(self):
        """
        Should normally return error counts.

        It is likely that anything that caused the upload to hit
        an internal error would have put the upload into an Error
        state. But, check anyway, just in case.
        """
        if self.internal_error:
            error = self.get_internal_error_prefix()
        else:
            error = self.msg if self.msg else f"Invalid status from run {self.run_id}"
        return {"error_message": error}

    def dataset_error(self):
        """
        Derived datasets need to write to primary dataset's error_message field;
        handle any errant primary datasets set to Error as well.
        """
        # if self.check_is_derived:
        #     msg = (
        #         self.msg
        #         if self.msg
        #         else f"Derived dataset {self.child_uuid} is in Error state."
        #     )
        # else:
        return {"error_message": self.get_internal_error_prefix()}

    def dataset_invalid(self):
        """
        Derived datasets should not be invalid but check just in case
        to ensure writing error to correct entity.
        """
        if self.internal_error:
            error = self.get_internal_error_prefix()
        else:
            error = self.msg if self.msg else f"Invalid status from run {self.run_id}"
        # if self.check_is_derived:
        #     error = f"Derived dataset {self.child_uuid} is in Invalid state. {error}"
        return {"error_message": error}

    ###########
    #
    # Utils
    #
    ###########

    # @property
    # def child_uuid(self) -> Optional[str]:
    #     """
    #     Check if dataset is descendant of another dataset.
    #     If so, set self.uuid to primary and return child uuid.
    #     """
    #     if primary_uuid := get_primary_dataset(self.entity_data, self.token):
    #         child_uuid = self.uuid
    #         self.uuid = primary_uuid
    #         return child_uuid
    #
    # @property
    # def check_is_derived(self) -> bool:
    #     return bool(self.child_uuid)
