import logging

from airflow.utils.email import send_email

from .status_utils import (
    Statuses,
    get_entity_ingest_url,
    get_project,
    get_submission_context,
    is_internal_error,
    log_directory_path,
)


class EmailManager:
    int_recipients = []  # TODO
    main_recipients = ""
    cc = ""
    subj = ""
    msg = ""

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
        self.msg = str(msg) if msg else None
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

    def get_message_content(self):
        if self.is_internal_error:  # error, potentially invalid
            subj, msg = self.internal_error_format()
        elif self.status in [
            "qa",
            "reorganized",
            "valid",
        ]:  # TODO: determine statuses requiring ext emails
            subj, msg = self.generic_good_status_format()
        elif self.status == "invalid":  # actually invalid
            subj, msg = self.get_ext_invalid_format()
        else:
            return
        self.subj = subj
        self.msg = msg

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

    def generic_good_status_format(self) -> tuple[str, str]:
        subj = (
            f"{self.entity_type} {self.entity_id} has successfully reached status {self.status}!"
        )
        msg = f"View ingest record: {get_entity_ingest_url(self.entity_data)}"
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
        </br>
        Error:<br>
            {self._parsed_error()}
        """
        return subj, msg

    def get_ext_invalid_format(self) -> tuple[str, str]:
        subj = f"{self.entity_type} {self.entity_id} is invalid"
        # TODO: ask about any instructions required here
        msg = f"""
        {self.project.value[1]} ID: {self.entity_id}<br>
        Group: {self.entity_data.get('group_name')}<br>
        Ingest page: {get_entity_ingest_url(self.entity_data)}<br>
        </br>
        {self.entity_type} is invalid:<br>
            {self._parsed_error()}
        """
        return subj, msg

    def get_recipients(self):
        self.main_recipients = ", ".join(self.int_recipients)
        self.cc = ", ".join(self.int_recipients)
        # TODO: turning off any ext emails for testing
        # if self.is_internal_error:
        #     self.main_recipients = ", ".join(self.int_recipients)
        # else:
        #     self.main_recipients = self.primary_contact
        #     self.cc = ", ".join(self.int_recipients)

    def _parsed_error(self):
        return "<br>     ".join(
            [line for line in self.entity_data.get("error_message", "").split("; ")]
        )
