import logging
from typing import TYPE_CHECKING, Optional

from .status_utils import (
    EntityUpdateException,
    Statuses,
    get_primary_dataset,
    get_submission_context,
    put_request_to_entity_api,
)

if TYPE_CHECKING:
    from submodules import ingest_validation_tools_error_report


class DataIngestBoardManager:
    # TODO: derived datasets are not created until pipeline steps succeed;
    # therefore there is no status change to indicate failed pipeline run.
    # Pipelines hit set_dataset_error but do not set error on primary.
    # Perhaps add EntityUpdater call *somewhere* in set_dataset_error?
    """
    Handle updating metadata fields tied to Data Ingest Board.
    """

    def __init__(
        self,
        status: Statuses,
        uuid: str,
        token: str,
        error_report: Optional["ingest_validation_tools_error_report.ErrorReport"],  # type: ignore
        *args,
        **kwargs,
    ):
        del args, kwargs
        self.uuid = uuid
        self.token = token
        self.status = status
        self.error_report = error_report
        self.entity_data = get_submission_context(self.token, self.uuid)
        self.update_fields = self.get_fields()

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
        msg_type = self.status.value
        if clear_msg := self.clear_only(msg_type, entity):
            return clear_msg
        func = getattr(self, msg_type, None)
        if not func:
            logging.info(
                f"Message method {msg_type} not found. Not updating metadata for {self.uuid}."
            )
            return
        return func()

    def clear_only(self, msg_type: str, entity: str) -> Optional[dict]:
        """
        Clear error messages following success.
        """
        clear_only = {
            "upload_reorganized",
            "upload_valid",
            "dataset_qa",
        }
        if msg_type in clear_only:
            if entity == "dataset":
                # Derived datasets need to write to primary dataset error_message;
                # set self.uuid to primary if uuid passed in is for a derived dataset.
                self.check_is_derived
            return {"error_message": ""}

    ########################
    #
    # Status-based messages
    #
    ########################

    def upload_error(self):
        return {
            "error_message": (
                self.counts if self.counts else f"Upload {self.uuid} is in Error state."
            )
        }

    def upload_invalid(self):
        return {
            "error_message": (
                self.counts if self.counts else f"Upload {self.uuid} is in Invalid state."
            )
        }

    def dataset_error(self):
        """
        Derived datasets need to write to primary dataset's error_message field;
        handle any errant primary datasets set to Error as well.
        """
        if self.check_is_derived:
            msg = (
                self.counts
                if self.counts
                else f"Derived dataset {self.child_uuid} is in Error state."
            )
        else:
            msg = f"Dataset {self.uuid} is in Error state."
        return {"error_message": msg}

    def dataset_invalid(self):
        """
        Derived datasets should not be invalid but check just in case
        to ensure writing error to correct entity.
        """
        if not self.check_is_derived:
            msg = self.counts if self.counts else f"Dataset {self.uuid} is in Invalid state."
        else:
            msg = f"Derived dataset {self.child_uuid} is in Invalid state."
        return {"error_message": msg}

    ###########
    #
    # Utils
    #
    ###########

    @property
    def counts(self) -> Optional[str]:
        if self.error_report:
            counts = self.error_report.counts()
            return "\n".join([f"{key}: {val}" for key, val in counts.items()])

    @property
    def child_uuid(self) -> Optional[str]:
        """
        Check if dataset is descendant of another dataset.
        If so, set self.uuid to primary and return child uuid.
        """
        if primary_uuid := get_primary_dataset(self.entity_data, self.token):
            child_uuid = self.uuid
            self.uuid = primary_uuid
            return child_uuid

    @property
    def check_is_derived(self) -> bool:
        return bool(self.child_uuid)
