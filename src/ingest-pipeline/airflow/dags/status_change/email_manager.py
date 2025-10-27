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
        "qa",
        "reorganized",
        "valid",
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
        self.status = Statuses.valid_str(status)
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
        elif self.status == "invalid":  # actually invalid
            subj, msg = self.get_ext_invalid_format()
        else:
            return
        self.subj = subj
        self.msg = msg
        if self.addtl_msg:
            self.msg += f"<br></br>{self.addtl_msg}"
        self.msg += "<br></br>This email address is not monitored. Please email ingest@hubmapconsortium.org with any questions about your data submission."

    def send_email(self):
        assert self.subj and self.msg
        logging.info(
            f"""
        Sending email
            Subject: {self.subj}
            Message: {self.msg}
            """
        )
        send_email(self.main_recipients, self.subj, self.msg, cc=self.cc)

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

    def generic_good_status_format(self) -> tuple[str, str]:
        subj = (
            f"{self.entity_type} {self.entity_id} has successfully reached status {self.status}!"
        )
        msg = f"""
        View ingest record: {get_entity_ingest_url(self.entity_data)}
        """
        return subj, msg

    def internal_error_format(self) -> tuple[str, str]:
        subj = f"Internal error for {self.entity_type} {self.entity_id}"
        msg = f"""
        {self.project.value[1]} ID: {self.entity_id}<br>
        UUID: {self.uuid}<br>
        Entity type: {self.entity_type}<br>
        Status: {self.status}<br>
        Group: {self.entity_data.get('group_name')}<br>
        Primary contact: {self.primary_contact}<br>
        Ingest page: {get_entity_ingest_url(self.entity_data)}<br>
        Log file: {log_directory_path(self.run_id)}<br>
        <br></br>
        Error:<br>
            {'<br>- '.join(split_error_counts(self.entity_data.get("error_message", "")))}
        """
        return subj, msg

    def get_ext_invalid_format(self) -> tuple[str, str]:
        subj = f"{self.entity_type} {self.entity_id} is invalid"
        msg = f"""
        {self.project.value[1]} ID: {self.entity_id}<br>
        Group: {self.entity_data.get('group_name')}<br>
        Ingest page: {get_entity_ingest_url(self.entity_data)}<br>
        <br></br>
        {self.entity_type} is invalid:<br>
            {'<br>- '.join(split_error_counts(self.entity_data.get("error_message", "")))}
        """
        return subj, msg
