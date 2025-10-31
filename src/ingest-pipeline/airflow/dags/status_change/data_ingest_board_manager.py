import logging
from typing import Optional

from .status_utils import (
    EntityUpdateException,
    Statuses,
    check_uuid_for_message_classes,
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

    def __init__(
        self,
        status: Statuses,
        token: str,
        uuid: Optional[str] = None,
        msg: str = "",
        run_id: str = "",
        primary_dataset: Optional[dict] = None,
        *args,
        **kwargs,
    ):
        del args, kwargs
        if error_msg := check_uuid_for_message_classes(
            status, uuid=uuid, primary_dataset=primary_dataset
        ):
            raise EntityUpdateException(error_msg)
        self.uuid = uuid
        self.token = token
        self.status = status
        self.msg = str(msg) if msg else ""
        self.run_id = run_id
        self.primary_dataset = primary_dataset or {}
        self.get_entity_data()
        self.entity_id_str = f"{get_project().value[0]}_id"  # "hubmap_id" or "sennet_id"
        self.update_fields = self.get_fields()
        self.is_valid_for_status = bool(self.update_fields)

    def update(self):
        if not self.update_fields or not self.uuid:
            return
        self.update_request()
        # TODO: test
        if self.set_on_derived:
            if self.status is Statuses.DATASET_ERROR:
                update_data = {
                    "error_message": self.dataset_error(),
                    "assigned_to_group_name": self.assigned_to_group_name,
                }
            elif self.status is Statuses.DATASET_QA:
                update_data = {"error_message": "", "assigned_to_group_name": ""}
                self.update_request(uuid=self.derived_uuid, update_fields=update_data)

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
        if self.status in self.clear_only:
            update_data = {"error_message": "", "assigned_to_group_name": ""}
        # TODO: test
        elif self.status in self.check_primary_before_update:
            if message := self.check_primary_field():
                update_data = {
                    "error_message": message,
                    "assigned_to_group_name": self.assigned_to_group_name,
                }
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

    # TODO: test
    def check_primary_field(self) -> Optional[str]:
        error_message = None
        existing_error_msg = str(self.primary_dataset.get("error_message"))
        error_prefix = "Derived dataset errors"
        derived_entity_id = str(self.derived_entity_data.get(self.entity_id_str))
        if Statuses.DATASET_ERROR:
            # Not a derived dataset error, continue
            if not self.primary_dataset:
                return
            if (error_prefix in existing_error_msg) and not (
                derived_entity_id in existing_error_msg
            ):
                error_message = f"{existing_error_msg}, {derived_entity_id}"
            else:
                error_message = f"{error_prefix}: {derived_entity_id}"
        elif Statuses.DATASET_QA:
            # Not a derived dataset QA, continue
            if not self.primary_dataset:
                return
            """
            - Retain any info not pertaining to this uuid.
            - Clear primary data related to this derived uuid.
            - Clear status on derived.
            """
            error_message = ""
            if existing_error_msg:
                for id_str in [f"{derived_entity_id}, ", derived_entity_id]:
                    if id_str in existing_error_msg:
                        error_message = existing_error_msg.replace(id_str, "")
        return error_message

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

    #########
    # Utils #
    #########

    def get_entity_data(self):
        """
        Get primary dataset info and reset self.uuid to primary uuid
        and self.entity_data to primary entity_data. Return derived
        dataset entity_data.
        """
        # If distinct parent uuid found, set derived_entity_data and reset
        # self.uuid/self.entity_data to primary to ensure we're writing to
        # the correct entity (derived errors set on primary, need to be
        # unset when derived reaches QA)
        self.entity_data = get_submission_context(self.token, self.uuid) if self.uuid else {}
        # TODO: test
        if self.primary_dataset:
            if self.uuid:
                self.set_on_derived = True
            self.derived_entity_data = self.entity_data
            self.derived_uuid = self.entity_data.get("uuid")
            self.entity_data = self.primary_dataset
            self.uuid = self.primary_dataset.get("uuid", "")
        if not self.uuid:
            raise EntityUpdateException(
                "DataIngestBoardManager could not find UUID, no update will be made."
            )
