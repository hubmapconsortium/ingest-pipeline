import json
from pprint import pprint
from requests.exceptions import HTTPError
from requests import codes
import traceback

from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.email import send_email

from utils import get_auth_tok


class FailureCallbackException(Exception):
    pass


class FailureCallback:
    """
    List should be overwritten by each subclass with appropriate values.
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
            self.send_notifications()

    def send_notifications(self):
        """
        Subclasses should override the send_notifications method
        in order to add self.send_asana_notification() or other
        custom notifications.
        """
        self.send_failure_email()

    # This is simplified from pythonop_get_dataset_state in utils
    def get_submission_context(self):
        method = "GET"
        headers = {
            "authorization": f"Bearer {self.auth_tok}",
            "content-type": "application/json",
            "X-Hubmap-Application": "ingest-pipeline",
        }
        http_hook = HttpHook(method, http_conn_id="entity_api_connection")

        endpoint = f"entities/{self.uuid}"

        try:
            response = http_hook.run(
                endpoint, headers=headers, extra_options={"check_response": False}
            )
            response.raise_for_status()
            submission_data = response.json()
            return submission_data
        except HTTPError as e:
            print(f"ERROR: {e}")
            if e.response.status_code == codes.unauthorized:
                raise RuntimeError("entity database authorization was rejected?")
            else:
                print("benign error")
                return {}

    def get_status_and_message(self):
        """
        Error message might need to be overwritten when
        subclassed for various DAGs.
        """
        return {
            "status": "Invalid",
            "validation_message": f"Process {self.dag_run.dag_id} started {self.dag_run.execution_date} failed at task {self.task.task_id} with error {self.exception_name} {self.exception}",
        }

    def set_status(self):
        """
        The failure callback needs to set the dataset status,
        otherwise it will remain in the "Processing" state
        """
        data = self.get_status_and_message()
        endpoint = f"/entities/{self.uuid}"
        headers = {
            "authorization": "Bearer " + self.auth_tok,
            "X-Hubmap-Application": "ingest-pipeline",
            "content-type": "application/json",
        }
        extra_options = []
        http_conn_id = "entity_api_connection"
        http_hook = HttpHook("PUT", http_conn_id=http_conn_id)
        print("data: ")
        pprint(data)
        response = http_hook.run(
            endpoint,
            json.dumps(data),
            headers,
            extra_options,
        )

    def get_failure_email_template(
        self, formatted_exception=None, external_template=False, submission_data=None, **kwargs
    ):
        """
        Generic template, can be overridden or super() called
        in subclass.
        """
        subject = f"DAG {self.dag_run.dag_id} failed at task {self.task.task_id}"
        if formatted_exception:
            msg = f"""
                             DAG run: {self.dag_run.id} {self.dag_run.dag_id} <br>
                             Task: {self.task.task_id} <br>
                             Execution date: {self.dag_run.execution_date} <br>
                             Run id: {self.dag_run.run_id} <br>
                             Error: {self.exception_name} <br>
                             Traceback: {formatted_exception}
                            """

        else:
            msg = f"""
                             DAG run: {self.dag_run.id} {self.dag_run.dag_id} <br>
                             Task: {self.task.task_id} <br>
                             Execution date: {self.dag_run.execution_date} <br>
                             Run id: {self.dag_run.run_id} <br>
                             Error: {self.exception_name} <br>
                             """
        return subject, msg

    def send_failure_email(self, **kwargs):
        # traceback logic borrowed from https://stackoverflow.com/questions/51822029/get-exception-details-on-airflow-on-failure-callback-context
        try:
            formatted_exception = "".join(
                traceback.TracebackException.from_exception(self.exception).format()
            ).replace("\n", "<br>")
        except:
            formatted_exception = None
        self.submission_data = self.get_submission_context()
        subject, msg = self.get_failure_email_template(
            formatted_exception=formatted_exception, submission_data=self.submission_data, **kwargs
        )
        send_email(to=self.internal_email_recipients, subject=subject, html_content=msg)
        if self.exception_name in self.external_exceptions:
            try:
                created_by_user_email = self.submission_data.get("created_by_user_email")
                subject, msg = self.get_failure_email_template(
                    formatted_exception=formatted_exception,
                    external_template=True,
                    submission_data=self.submission_data,
                    **kwargs,
                )
                send_email(to=[created_by_user_email], subject=subject, html_content=msg)
            except:
                raise FailureCallbackException(
                        "Failure retrieving created_by_user_email or sending email."
                )

    def send_asana_notification(self, **kwargs):
        pass
