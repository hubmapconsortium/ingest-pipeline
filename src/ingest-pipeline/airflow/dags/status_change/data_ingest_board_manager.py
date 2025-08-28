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
    """
    Handle updating metadata fields tied to Data Ingest Board.
    """

    def __init__(
        self,
        status: Statuses,
        uuid: str,
        token: str,
        error_report: Optional["ingest_validation_tools_error_report.ErrorReport"],
        *args,
        **kwargs,
    ):
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
        find_func = f"{entity}_{self.status}"
        func = getattr(self, find_func, None)
        if not func:
            logging.info(
                f"Message method {find_func} not found. Not updating metadata for {self.uuid}."
            )
            return
        return func()

    ########################
    #
    # Status-based messages
    #
    ########################

    def upload_invalid(self):
        if self.error_report:
            counts = self.error_report.counts()
            msg = "\n".join([f"{key}: {val}" for key, val in counts.items()])
        else:
            msg = f"Upload {self.uuid} is in Invalid state."
        return {"error_message": msg}

    def dataset_invalid(self):
        if primary_uuid := get_primary_dataset(self.entity_data):
            # Derived datasets shouldn't reach this state so in this
            # case we don't care about error report, just report Invalid.
            child_uuid = self.uuid
            self.uuid = primary_uuid
            msg = f"Derived dataset {child_uuid} is in Invalid state."
        elif self.error_report:
            counts = self.error_report.counts()
            msg = "\n".join([f"{key}: {val}" for key, val in counts.items()])
        else:
            msg = f"Dataset {self.uuid} is in Invalid state."
        return {"error_message": msg}

    def upload_error(self):
        if self.error_report:
            counts = self.error_report.counts()
            msg = "\n".join([f"{key}: {val}" for key, val in counts.items()])
        else:
            msg = f"Upload {self.uuid} is in Error state."
        return {"error_message": msg}

    def dataset_error(self):
        """
        Derived datasets need to write to primary dataset's error_message field;
        handle any errant primary datasets set to Error as well.
        """
        if primary_uuid := get_primary_dataset(self.entity_data):
            child_uuid = self.uuid
            self.uuid = primary_uuid
            return {"error_message": f"Derived dataset {child_uuid} is in Error state."}
        return {"error_message": f"Dataset {self.uuid} is in Error state."}

    def dataset_approved(self):
        """
        Clear error message on approval (if UUID is for a derived dataset,
        clear error message on primary dataset).
        """
        if primary_uuid := get_primary_dataset(self.entity_data):
            self.uuid = primary_uuid
        return {"error_message": ""}
