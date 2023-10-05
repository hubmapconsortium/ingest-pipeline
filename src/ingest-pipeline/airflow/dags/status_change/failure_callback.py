import logging
from pprint import pprint

from status_change.send_emails import SendFailureEmail
from status_change.status_manager import StatusChanger
from status_change.status_utils import formatted_exception
from utils import get_auth_tok


class FailureCallbackException(Exception):
    pass


class FailureCallback:
    """
    List should be overridden by each subclass with appropriate values.
    """

    def __init__(self, context):
        self.context = context
        self.uuid = self.context.get("task_instance").xcom_pull(key="uuid")
        self.auth_tok = get_auth_tok(**context)
        self.dag_run = self.context.get("dag_run")
        self.task = self.context.get("task")
        exception = self.context.get("exception")
        self.formatted_exception = formatted_exception(exception)
        self.notification_instance = SendFailureEmail(self.context)

        self.pre_set_status()

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

    def pre_set_status(self):
        # Allows for alterations to props, e.g. notification_instance,
        # before calling StatusChanger
        self.set_status()

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
            notification_instance=self.notification_instance,
        ).on_status_change()
