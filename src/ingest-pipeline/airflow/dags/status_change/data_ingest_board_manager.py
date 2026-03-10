import logging

from .status_utils import (
    EntityUpdateException,
    MessageManager,
    Statuses,
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
        messages: dict | None = None,
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

    def get_fields(self) -> dict | None:
        entity = self.entity_data.get("entity_type", "").lower()
        msg_type = f"{entity}_{Statuses.valid_str(self.status)}"
        if clear_msg := self.get_clear_message(msg_type):
            return clear_msg
        func = getattr(self, msg_type, None)
        if not func:
            logging.info(
                f"Message method {msg_type} not found. Not updating metadata for {self.uuid}."
            )
            return
        update_data = func()
        update_data["assigned_to_group_name"] = self.assigned_to_group_name
        return update_data

    def get_clear_message(self, msg_type: str) -> dict | None:
        """
        Clear error messages following success.
        """
        if msg_type in self.clear_only:
            return {"error_message": ""}

    @property
    def assigned_to_group_name(self) -> str:
        if group_name := self.entity_data.get("group_name"):
            if self.status in self.assign_to_dp:
                return group_name
        return ""

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
        Derived datasets need to write to primary dataset's error_message field;
        handle any errant primary datasets set to Error as well.
        """
        return {"error_message": self.get_internal_error_msg()}

    def dataset_invalid(self):
        """
        Derived datasets should not be invalid but check just in case
        to ensure writing error to correct entity.
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
