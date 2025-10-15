import logging
from typing import Optional

from airflow.utils.email import send_email

from .status_utils import (
    EntityUpdateException,
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
        self.is_valid_for_status = bool(self.message_content)
        self.primary_contact = [self.entity_data.get("created_by_user_email", "")]

    def update(self):
        if not self.message_content:
            logging.error(
                "Status is valid for EmailManager but no message content available. Exiting without sending."
            )
            return
        self.get_recipients()
        self.send_email(*self.message_content)

    @property
    def message_content(self) -> Optional[tuple[str, str]]:
        if self.is_internal_error:  # error, potentially invalid
            subj, msg = self.internal_error_format()
        elif self.status in [
            "qa",
            "reorganized",
            "valid",
        ]:  # TODO: determine statuses requiring ext emails
            subj, msg = self.generic_good_status_format()
        else:  # actually invalid
            subj, msg = self.get_ext_invalid_format()
        if subj and msg:
            return subj, msg
        elif subj or msg:
            raise EntityUpdateException(
                f"Both subject and message content required. Received subject: {subj}, message: {msg}"
            )

    def send_email(self, subj: str, msg: str):
        send_email(self.main_recipients, subj, msg, cc=self.cc)

    def generic_good_status_format(self) -> tuple[str, str]:
        subj = (
            f"{self.entity_type} {self.entity_id} has successfully reached status {self.status}!"
        )
        msg = f"View ingest record: {get_entity_ingest_url(self.entity_data)}"
        return subj, msg

    def internal_error_format(self) -> tuple[str, str]:
        subj = f"Internal error for {self.entity_type} {self.entity_id}"
        msg = f"""
        {self.project.value[1]} ID: {self.entity_id}
        UUID: {self.uuid}
        Entity type: {self.entity_type}
        Status: {self.status}
        Group: {self.entity_data.get('group_name')}
        Primary contact: {" | ".join([f'{name}: {email}' for name, email in self.primary_contact])}
        Ingest page: {get_entity_ingest_url(self.entity_data)}
        Log file: {log_directory_path(self.run_id)}

        Error: {self.entity_data.get('error_message')}
        """
        return subj, msg

    def get_ext_invalid_format(self) -> tuple[str, str]:
        subj = f"{self.entity_type} {self.entity_id} is invalid"
        # TODO: ask about any instructions required here
        msg = f"""
        {self.project.value[1]} ID: {self.entity_id}
        Group: {self.entity_data.get('group_name')}
        Ingest page: {get_entity_ingest_url(self.entity_data)}

        {self.entity_type} is invalid:
            {self.entity_data.get('error_message')}
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
