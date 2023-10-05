import logging
import traceback
from functools import cached_property
from pprint import pprint

from status_change.status_manager import StatusChanger
from status_change.status_utils import get_submission_context
from utils import get_auth_tok

from airflow.utils.email import send_email


class FailureCallbackException(Exception):
    pass


class FailureCallback:
    """
    List should be overridden by each subclass with appropriate values.
    """

    external_exceptions = []
    # TODO: Switch to curator email(s)
    internal_email_recipients = ["gesina@psc.edu"]

    def __init__(self, context, execute_methods=True):
        self.context = context
        self.uuid = self.context.get("task_instance").xcom_pull(key="uuid")
        self.auth_tok = get_auth_tok(**context)
        self.dag_run = self.context.get("dag_run")
        self.task = self.context.get("task")
        self.exception = self.context.get("exception")
        self.exception_name = type(self.exception).__name__

        if execute_methods:
            self.set_status()
            # Not sending notifications currently, and
            # notification responsibility could potentially move to status_manager.
            # self.send_notifications()

    def send_notifications(self):
        """
        Subclasses should override the send_notifications method
        in order to add self.send_asana_notification() or other
        custom notifications.
        """
        self.send_failure_email()

    def get_extra_fields(self):
        """
        Error message might need to be overwritten when
        subclassed for various DAGs.
        'Error' is the default for FailureCallback, which indicates a pipeline has failed.
        """
        return {
            "validation_message": f"""
                Process {self.dag_run.dag_id} started {self.dag_run.execution_date}
                failed at task {self.task.task_id}.
                {f'Error: {self.formatted_exception}' if self.formatted_exception else ""}
            """,
        }

    def set_status(self):
        """
        The failure callback needs to set the dataset status,
        otherwise it will remain in the "Processing" state
        """
        data = self.get_extra_fields()
        logging.info("data: ")
        logging.info(pprint(data))
        StatusChanger(
            self.uuid,
            self.auth_tok,
            "error",
            {
                "extra_fields": self.get_extra_fields(),
                "extra_options": {},
            },
        ).on_status_change()

    def get_internal_email_template(self, formatted_exception=None):
        """
        Generic template, can be overridden or super() called
        in subclass.
        """
        subject = f"DAG {self.dag_run.dag_id} failed at task {self.task.task_id}"
        msg = f"""
                DAG run: {self.dag_run.id} {self.dag_run.dag_id} <br>
                Task: {self.task.task_id} <br>
                Execution date: {self.dag_run.execution_date} <br>
                Run id: {self.dag_run.run_id} <br>
                Error: {self.exception_name} <br>
                {f'Traceback: {formatted_exception}' if formatted_exception else None}
            """
        return subject, msg

    def get_external_email_template(self):
        raise NotImplementedError

    @cached_property
    def get_external_email_recipients(self):
        if self.exception_name in self.external_exceptions:
            try:
                created_by_user_email = get_submission_context.get("created_by_user_email")
                assert isinstance(created_by_user_email, str)
                return [created_by_user_email]
            except AssertionError:
                raise FailureCallbackException(
                    f"Failed to retrieve creator email address for {self.uuid}."
                )

    @cached_property
    def formatted_exception(self):
        """
        traceback logic from
        https://stackoverflow.com/questions/51822029/get-exception-details-on-airflow-on-failure-callback-context
        """
        if not (
            formatted_exception := "".join(
                traceback.TracebackException.from_exception(self.exception).format()
            ).replace("\n", "<br>")
        ):
            return None
        return formatted_exception

    def send_failure_email(self) -> None:
        """
        This only sends to internal recipients, and would need to be overridden
        to use get_external_email_recipients and get_external_email_template.
        """
        subject, msg = self.get_internal_email_template(
            formatted_exception=self.formatted_exception
        )
        self.send_email(self.internal_email_recipients, subject, msg)

    def send_email(self, recipients: list, subject: str, msg: str) -> None:
        logging.info(
            f"""
                Sending failure notification to {recipients}...
                Subject: {subject}
                Message: {msg}
            """
        )
        try:
            send_email(to=recipients, subject=subject, html_content=msg)
            logging.info("Email sent successfully!")
        except Exception as e:
            raise FailureCallbackException(
                f"""
                Failure sending email.
                Recipients: {self.internal_email_recipients}
                Error: {e}
                """
            )

    def send_asana_notification(self):
        pass
