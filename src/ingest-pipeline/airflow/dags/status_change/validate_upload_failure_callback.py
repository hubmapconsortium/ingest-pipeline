from functools import cached_property
from typing import Tuple, Union

from status_change.send_emails import SendFailureEmail
from status_change.status_utils import get_submission_context

from .failure_callback import FailureCallback


class ValidateUploadFailure(FailureCallback):
    def pre_set_status(self) -> None:
        self.notification_instance = ValidateUploadFailureEmail(self.context)
        super().set_status()


class ValidateUploadFailureEmail(SendFailureEmail):
    # Comparing strings here is ugly, should be importing
    external_exceptions = [
        "ValueError",
        "PreflightError",
        "ValidatorError",
        "DirectoryValidationErrors",
        "FileNotFoundError",
    ]

    @cached_property
    def get_external_email_template(self) -> Tuple:
        if report_txt := get_submission_context.get("report_txt"):
            subject = (
                f"{get_submission_context.get('entity_type')} {self.context['uuid']} has failed!"
            )
            msg = f"""
                Error: {report_txt}
                """
        else:
            subject = (
                f"{get_submission_context.get('entity_type')} {self.context['uuid']} has failed!"
            )
            msg = f"""
                Error: {self.formatted_exception if self.formatted_exception else 'Unknown Error'}
                """
        return subject, msg

    def send_failure_email(self, offline: bool) -> Union[str, None]:
        """
        Currently we don't want to send any emails.
        """
        super().send_failure_email(offline)
        if self.get_external_email_recipients and self.get_external_email_template:
            subject, msg = self.get_external_email_template
            if not offline:
                self.send_email(self.get_external_email_recipients, subject, msg)
            return f"""
                    External recipients: {self.get_external_email_recipients}
                    External subject: {subject}
                    External message: {msg}
                """
