import logging
from functools import cached_property
from typing import Any, Dict, List

from status_change.failure_callback import FailureCallbackException
from status_change.status_utils import formatted_exception, get_submission_context

from airflow.utils.email import send_email


class SendEmail:
    """
    A minimal implementation of a SendEmail subclass would
    define internal recipients, add a template or subject/msg
    vars, and use send_notifications directly to call
    self.send_email with those recipients/template args.
    The standard way of interacting with SendEmail and its Subclasses
    is SendEmail(context).send_notifications().
    """

    internal_email_recipients: List

    def __init__(self, context: Dict[str, Any]):
        self.context = context
        self.dag_run = self.context["dag_run"]
        self.task = self.context["task"]

    def get_internal_email_template(self):
        """
        Generic template, must be overridden.
        """
        raise NotImplementedError()

    def send_notifications(self):
        """
        Subclasses should override the send_notifications method
        to add the appropriate send_email method(s).
        """
        raise NotImplementedError()

    def send_email(self, recipients: list, subject: str, msg: str) -> None:
        logging.info(
            f"""
                Sending notification to {recipients}...
                Subject: {subject}
                Message: {msg}
            """
        )
        try:
            send_email(to=recipients, subject=subject, html_content=msg)
            logging.info("Email sent successfully!")
        except Exception as e:
            # TODO: custom exception?
            raise Exception(
                f"""
                Failure sending email.
                Recipients: {self.internal_email_recipients}
                Error: {e}
                """
            )


class SendFailureEmail(SendEmail):
    """
    This will likely need to be subclassed by error type and/or source DAG?
    """

    external_exceptions = []
    # This will likely be specific per DAG
    # internal_email_recipients = []

    def __init__(self, context: Dict[str, Any]):
        super().__init__(context)
        self.exception = self.context.get("exception")
        self.exception_name = type(self.exception).__name__
        self.formatted_exception = formatted_exception(self.exception)

    def send_notifications(self, offline=True):
        self.send_failure_email(offline)

    def get_internal_email_template(self):
        subject = f"DAG {self.dag_run.dag_id} failed at task {self.task.task_id}"
        msg = f"""
                DAG run: {self.dag_run.id} {self.dag_run.dag_id} <br>
                Task: {self.task.task_id} <br>
                Execution date: {self.dag_run.execution_date} <br>
                Run id: {self.dag_run.run_id} <br>
                Error: {self.exception_name} <br>
                {f'Traceback: {self.formatted_exception}' if self.formatted_exception else None}
            """
        return subject, msg

    def get_external_email_template(self):
        raise NotImplementedError

    @cached_property
    # TODO: is created_by_user the right call here,
    # or is there a more general contact field for all
    # corresponding submitters?
    def get_external_email_recipients(self):
        if self.exception_name in self.external_exceptions:
            try:
                created_by_user_email = get_submission_context.get("created_by_user_email")
                assert isinstance(created_by_user_email, str)
                return [created_by_user_email]
            except AssertionError:
                raise FailureCallbackException(
                    f"Failed to retrieve creator email address for {self.context.get('uuid')}."
                )

    def send_failure_email(self, offline: bool) -> str:
        """
        This only sends to internal recipients, and would need to be overridden
        to use get_external_email_recipients and get_external_email_template.
        """
        subject, msg = self.get_internal_email_template()
        if offline:
            logging.info("Email that would have been sent:")
        else:
            self.send_email(self.internal_email_recipients, subject, msg)
            logging.info("Email sent!")
        return f"""
            Internal recipients: {self.internal_email_recipients}
            Internal subject: {subject}
            Internal message: {msg}
        """
