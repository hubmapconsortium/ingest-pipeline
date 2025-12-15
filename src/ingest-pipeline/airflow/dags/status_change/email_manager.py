import logging
from typing import Optional

from status_change.email_templates.error import ErrorStatusEmail
from status_change.email_templates.good import GenericGoodStatusEmail
from status_change.email_templates.invalid import InvalidStatusEmail
from status_change.email_templates.reorganized import ReorganizedStatusEmail
from status_change.status_utils import (
    MessageManager,
    Statuses,
    get_project,
)

from airflow.configuration import conf as airflow_conf
from airflow.utils.email import send_email


class EmailManager(MessageManager):
    int_recipients = ["bhonick@psc.edu"]
    main_recipients = ""
    cc = ""
    subj = ""
    msg = ""
    good_statuses = [
        Statuses.DATASET_QA,
        Statuses.UPLOAD_VALID,
    ]

    def __init__(
        self,
        status: Statuses,
        uuid: str,
        token: str,
        messages: Optional[dict] = None,
        run_id: str = "",
        *args,
        **kwargs,
    ):
        super().__init__(status, uuid, token, messages, run_id, args, kwargs)
        self.entity_type = self.entity_data.get("entity_type", "").title()
        self.project = get_project()
        self.entity_id = self.entity_data.get(f"{get_project().value[0]}_id")
        self.primary_contact = self.entity_data.get("created_by_user_email", "")
        self.get_message_content()

    @property
    def is_valid_for_status(self):
        return bool(self.subj and self.msg)

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

    ###################
    # Message details #
    ###################

    def get_message_content(self) -> Optional[tuple[str, str]]:
        # error status or bad content in validation_message
        if self.is_internal_error:
            self.subj, self.msg = ErrorStatusEmail(self).format()
        # good status or reorg with child datasets
        elif self.status in self.good_statuses:
            self.subj, self.msg = GenericGoodStatusEmail(self).format()
        # finished reorg (has datasets)
        elif self.reorg_status_with_child_datasets():
            self.subj, self.msg = ReorganizedStatusEmail(self).format()
        # actually invalid
        elif self.status in [
            Statuses.DATASET_INVALID,
            Statuses.UPLOAD_INVALID,
        ]:
            self.subj, self.msg = InvalidStatusEmail(self).format()
        else:
            return

    def get_recipients(self):
        self.get_config_values()
        if self.is_internal_error:
            self.main_recipients = ", ".join(self.int_recipients)
        else:
            self.main_recipients = self.primary_contact
            self.main_recipients = ", ".join(self.int_recipients)
            self.cc = ", ".join(self.int_recipients)

    #########
    # Tests #
    #########

    def reorg_status_with_child_datasets(self):
        if self.status == Statuses.UPLOAD_REORGANIZED and self.entity_data.get("datasets"):
            return True  # only want to send good email if reorg status AND has child datasets
        logging.info(
            "Reorganized upload does not have child datasets (DAG may still be running); not sending email."
        )
        return False

    #########
    # Utils #
    #########

    def get_config_values(self):
        conf_dict = airflow_conf.as_dict()
        # Allows for setting defaults at the config level that override class defaults, e.g. for testing
        if int_recipients := conf_dict.get("email_notifications", {}).get("int_recipients"):
            cleaned_int_recipients = [str(address) for address in [int_recipients]]
            self.int_recipients = cleaned_int_recipients
        if main_recipient := conf_dict.get("email_notifications", {}).get("main"):
            self.primary_contact = main_recipient
