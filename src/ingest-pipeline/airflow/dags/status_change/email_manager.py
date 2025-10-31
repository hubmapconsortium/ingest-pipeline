import logging
from typing import Optional

from airflow.configuration import conf as airflow_conf
from airflow.utils.email import send_email

from .status_utils import (
    EntityUpdateException,
    Statuses,
    check_uuid_for_message_classes,
    get_entity_ingest_url,
    get_project,
    get_submission_context,
    is_internal_error,
    log_directory_path,
    split_error_counts,
)


class EmailManager:
    int_recipients = ["bhonick@psc.edu"]
    main_recipients = ""
    cc = ""
    subj = ""
    msg = ""
    primary_only = False
    good_statuses = [
        Statuses.DATASET_QA,
        Statuses.UPLOAD_VALID,
    ]
    footer = [
        "",
        "This email address is not monitored. Please email ingest@hubmapconsortium.org with any questions about your data submission.",
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
        self.addtl_msg = str(msg) if msg else None
        self.run_id = run_id
        self.primary_dataset = primary_dataset or {}
        self.project = get_project()
        self.entity_id_str = f"{self.project.value[0]}_id"
        # TODO: test
        if self.uuid:
            self.entity_data = get_submission_context(self.token, self.uuid)
        else:
            # Error in pipeline before dataset created, no derived uuid
            self.primary_only = True
            self.uuid = self.primary_dataset.get("uuid")
            self.entity_data = self.primary_dataset
        self.is_internal_error = is_internal_error(self.entity_data)
        self.entity_type = self.entity_data.get("entity_type", "").title()
        self.entity_id = self.entity_data.get(self.entity_id_str)
        self.primary_contact = self.entity_data.get("created_by_user_email", "")
        self.get_message_content()
        self.is_valid_for_status = bool(self.subj and self.msg)

    def update(self):
        if not (self.subj and self.msg):
            logging.error(
                f"""
            Status is valid for EmailManager but missing full message content.
            Subject: {self.subj}.
            Message: {self.msg}
            Exiting without sending.
            """
            )
            return
        self.get_recipients()
        self.send_email()

    def get_message_content(self) -> Optional[tuple[str, str]]:
        if self.is_internal_error:  # error status or bad content in validation_message
            # TODO: test
            if self.primary_only:  # no uuid, only primary_dataset
                self.subj, msg = self.internal_primary_only_error_format()
                self.uuid = self.primary_dataset.get("uuid")
                self.entity_data = self.primary_dataset
            else:
                self.subj, msg = self.internal_error_format()
        elif (
            self.status in self.good_statuses or self.reorg_status_with_child_datasets()
        ):  # good status or reorg with child datasets
            self.subj, msg = self.generic_good_status_format()
        elif self.status in [
            Statuses.DATASET_INVALID,
            Statuses.UPLOAD_INVALID,
        ]:  # actually invalid
            self.subj, msg = self.get_ext_invalid_format()
        else:
            return
        if self.addtl_msg and self.addtl_msg != self.entity_data.get("error_message"):
            msg.append(self.addtl_msg)
        if self.primary_dataset and not self.primary_only:
            msg.append(
                f"Primary dataset: <a href='{get_entity_ingest_url(self.primary_dataset)}'>{self.primary_dataset.get(self.entity_id_str)}</a>"
            )
        msg.extend(self.footer)
        self.msg = msg

    def send_email(self):
        assert self.subj and self.msg
        msg_str = "<br>".join([line.lstrip() for line in self.msg])
        logging.info(
            f"""
        Sending email
            Subject: {self.subj}
            Message: {msg_str}
            """
        )
        send_email(self.main_recipients, self.subj, msg_str, cc=self.cc)

    def get_recipients(self):
        # Allows for setting defaults at the config level that override class defaults, e.g. for testing
        conf_dict = airflow_conf.as_dict()
        if int_recipients := conf_dict.get("email_notifications", {}).get("int_recipients"):
            cleaned_int_recipients = [str(address) for address in [int_recipients]]
            self.int_recipients = cleaned_int_recipients
        if main_recipient := conf_dict.get("email_notifications", {}).get("main"):
            self.primary_contact = main_recipient
        if self.is_internal_error:
            self.main_recipients = ", ".join(self.int_recipients)
        else:
            self.main_recipients = self.primary_contact
            self.cc = ", ".join(self.int_recipients)

    #############
    # Templates #
    #############

    def generic_good_status_format(self) -> tuple[str, list]:
        subj = f"{self.entity_type} {self.entity_id} has successfully reached status {self.status.titlecase}!"
        msg = [f"View ingest record: {get_entity_ingest_url(self.entity_data)}"]
        return subj, msg

    def internal_primary_only_error_format(self) -> tuple[str, list]:
        # TODO: test
        primary_entity_id = self.primary_dataset.get(self.entity_id_str)
        subj = f"Error occurred in pipeline for primary dataset {primary_entity_id}"
        msg = [
            f"{self.project.value[1]} ID: {primary_entity_id}",
            f"UUID: {self.primary_dataset.get('uuid')}",
            f"Entity type: dataset",
            f"Status: error",
            f"Group: {self.primary_dataset.get('group_name')}",
            f"Ingest page: {get_entity_ingest_url(self.primary_dataset)}",
            f"Log file: {log_directory_path(self.run_id)}",
        ]
        if error_message := self.primary_dataset.get("error_message"):
            msg.extend(["", "Error:"])
            msg.extend(split_error_counts(error_message, no_bullets=True))
        return subj, msg

    def internal_error_format(self) -> tuple[str, list]:
        subj = f"Internal error for {self.entity_type} {self.entity_id}"
        msg = [
            f"{self.project.value[1]} ID: {self.entity_id}",
            f"UUID: {self.uuid}",
            f"Entity type: {self.entity_type}",
            f"Status: {self.status.titlecase}",
            f"Group: {self.entity_data.get('group_name')}",
            f"Primary contact: {self.primary_contact}",
            f"Ingest page: {get_entity_ingest_url(self.entity_data)}",
            f"Log file: {log_directory_path(self.run_id)}",
        ]
        if error_message := self.entity_data.get("error_message"):
            msg.extend(["", "Error:"])
            msg.extend(split_error_counts(error_message, no_bullets=True))
        return subj, msg

    def get_ext_invalid_format(self) -> tuple[str, list]:
        subj = f"{self.entity_type} {self.entity_id} is invalid"
        msg = [
            f"{self.project.value[1]} ID: {self.entity_id}",
            f"Group: {self.entity_data.get('group_name')}",
            f"Ingest page: {get_entity_ingest_url(self.entity_data)}",
        ]
        if error_message := self.entity_data.get("error_message"):
            msg.extend(["", f"{self.entity_type} is invalid:"])
            msg.extend(split_error_counts(error_message))
        return subj, msg

    #########
    # Tests #
    #########

    def reorg_status_with_child_datasets(self):
        if self.status == Statuses.UPLOAD_REORGANIZED and self.entity_data.get("datasets"):
            logging.info(
                "Reorganized upload does not have child datasets (DAG may still be running); not sending email."
            )
            return True  # only want to send good email if reorg status AND has child datasets
        return False
