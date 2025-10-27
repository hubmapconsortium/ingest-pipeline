import logging
from typing import Optional

from airflow.utils.email import send_email

from .status_utils import (
    Statuses,
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
    good_statuses = [
        Statuses.DATASET_QA,
        Statuses.UPLOAD_REORGANIZED,
        Statuses.UPLOAD_VALID,
    ]

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
        self.addtl_msg = str(msg) if msg else None
        self.run_id = run_id
        self.entity_data = get_submission_context(self.token, self.uuid)
        self.entity_type = self.entity_data.get("entity_type", "").title()
        self.project = get_project()
        self.is_internal_error = is_internal_error(self.entity_data)
        self.entity_id = self.entity_data.get(f"{get_project().value[0]}_id")
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
            subj, msg = self.internal_error_format()
        elif self.status in self.good_statuses:
            subj, msg = self.generic_good_status_format()
        elif self.status in [
            Statuses.DATASET_INVALID,
            Statuses.UPLOAD_INVALID,
        ]:  # actually invalid
            subj, msg = self.get_ext_invalid_format()
        else:
            return
        self.subj = subj
        if self.addtl_msg and self.addtl_msg != self.entity_data.get("error_message"):
            msg.append(self.addtl_msg)
        msg.extend(
            [
                "",
                "This email address is not monitored. Please email ingest@hubmapconsortium.org with any questions about your data submission.",
            ]
        )
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
        if self.is_internal_error:
            self.main_recipients = ", ".join(self.int_recipients)
        else:
            # TODO: turning off any ext emails for testing
            # self.main_recipients = self.primary_contact
            self.main_recipients = ", ".join(self.int_recipients)
            self.cc = ", ".join(self.int_recipients)

    #############
    # Templates #
    #############

    def generic_good_status_format(self) -> tuple[str, list]:
        subj = f"{self.entity_type} {self.entity_id} has successfully reached status {self.status.titlecase}!"
        msg = [f"View ingest record: {get_entity_ingest_url(self.entity_data)}"]
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
