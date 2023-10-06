import logging
from functools import cached_property
from typing import Any, Dict, List, Optional, Tuple

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

    def __init__(self, context: Dict[str, Any]):
        self.context = context
        self.internal_email_recipients = []

    def get_internal_email_template(self) -> Tuple:
        """
        Generic template, should be overridden.
        """
        subject = "Status change details"
        msg_strings = [f"{k}: {v} <br>" for k, v in self.context.items()]
        msg = "".join(x for x in msg_strings)
        return subject, msg

    def send_notifications(self):
        """
        Subclasses should override the send_notifications method
        to add the appropriate send_email method(s).
        """
        subject, msg = self.get_internal_email_template()
        self.send_email(self.internal_email_recipients, subject, msg)

    def send_email(
        self, recipients: List[str], subject: str, msg: str, offline: bool = True
    ) -> None:
        logging.info(
            f"""
                Sending notification to {recipients}...
                Subject: {subject}
                Message: {msg}
            """
        )
        if not offline:
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
        self.dag_run = self.context.get("dag_run")
        self.task = self.context.get("task")
        self.exception = self.context.get("exception")
        if self.exception:
            self.exception_name = type(self.exception).__name__
        self.formatted_exception = formatted_exception(self.exception)

    def send_notifications(self, offline=True):
        self.send_failure_email(offline)

    def get_internal_email_template(self) -> Tuple:
        if isinstance(self.dag_run, dict) and isinstance(self.task, dict):
            subject = f"DAG {self.dag_run.get('dag_id')} failed at task {self.task.get('task_id')}"
            # TODO: ugh timezones
            msg = f"""
                    DAG: {self.dag_run.get("dag_id")} <br>
                    Task: {self.task.get("task_id")} <br>
                    Execution date: {self.dag_run.get("execution_date")} <br>
                    Run ID: {self.dag_run.get("run_id")} <br>
                    Error: {self.exception_name if self.exception_name else 'Unknown'} <br>
                    {f'Traceback: {self.formatted_exception}' if self.formatted_exception else None}
                """
            return subject, msg
        else:
            return super().get_internal_email_template()

    def get_external_email_template(self):
        raise NotImplementedError

    @cached_property
    # TODO: is created_by_user the right call here,
    # or is there a more general contact field for all
    # corresponding submitters?
    def get_external_email_recipients(self) -> Optional[List]:
        if self.exception_name in self.external_exceptions:
            try:
                created_by_user_email = get_submission_context.get("created_by_user_email")
                assert isinstance(created_by_user_email, str)
                return [created_by_user_email]
            except AssertionError:
                raise Exception(
                    f"Failed to retrieve creator email address for {self.context.get('uuid')}."
                )

    def send_failure_email(self, offline: bool) -> None:
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
        logging.info(
            f"""
            Internal recipients: {self.internal_email_recipients}
            Internal subject: {subject}
            Internal message: {msg}
        """
        )
