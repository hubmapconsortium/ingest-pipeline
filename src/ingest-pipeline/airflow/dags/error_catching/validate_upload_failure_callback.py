from airflow.utils.email import send_email

from failure_callback import FailureCallback, FailureCallbackException


class ValidateUploadFailure(FailureCallback):
    # Should probably be importing custom exceptions rather than comparing strings
    external_exceptions = [
        "ValueError",
        "PreflightError",
        "ValidatorError",
        "DirectoryValidationErrors",
        "FileNotFoundError",
    ]

    def get_failure_email_template(
        self,
        formatted_exception=None,
        external_template=False,
        submission_data=None,
        report_txt=False,
    ):
        if external_template:
            if report_txt and submission_data:
                subject = f"Your {submission_data.get('entity_type')} has failed!"
                msg = f"""
                    Error: {report_txt}
                    """
                return subject, msg
        else:
            if report_txt and submission_data:
                subject = f"{submission_data.get('entity_type')} {self.uuid} has failed!"
                msg = f"""
                    Error: {report_txt}
                    """
                return subject, msg
        return super().get_failure_email_template(formatted_exception)

    def send_failure_email(self, **kwargs):
        super().send_failure_email(**kwargs)
        if "report_txt" in kwargs:
            try:
                created_by_user_email = self.submission_data.get("created_by_user_email")
                subject, msg = self.get_failure_email_template(
                    formatted_exception=None,
                    external_template=True,
                    submission_data=self.submission_data,
                    **kwargs,
                )
                send_email(to=[created_by_user_email], subject=subject, html_content=msg)
            except:
                raise FailureCallbackException(
                    "Failure retrieving created_by_user_email or sending email in ValidateUploadFailure."
                )
