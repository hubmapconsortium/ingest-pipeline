from functools import cached_property

from status_change.status_utils import get_submission_context

from .failure_callback import FailureCallback


class ValidateUploadFailure(FailureCallback):
    # Comparing strings here is ugly, should be importing
    external_exceptions = [
        "ValueError",
        "PreflightError",
        "ValidatorError",
        "DirectoryValidationErrors",
        "FileNotFoundError",
    ]

    @cached_property
    def get_external_email_template(self):
        if report_txt := get_submission_context.get("report_txt"):
            subject = f"{get_submission_context.get('entity_type')} {self.uuid} has failed!"
            msg = f"""
                Error: {report_txt}
                """
            return subject, msg
        else:
            return None

    def send_failure_email(self):
        """
        Currently we don't want to send any emails.
        """
        pass
        # super().send_failure_email()
        # if self.get_external_email_recipients and self.get_external_email_template:
        #     subject, msg = self.get_external_email_template
        #     self.send_email(self.get_external_email_recipients, subject, msg)
