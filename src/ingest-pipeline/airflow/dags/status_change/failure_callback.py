import logging
from pprint import pformat

from status_change.status_manager import StatusChanger
from status_change.status_utils import formatted_exception
from utils import get_auth_tok


class FailureCallbackException(Exception):
    pass


class FailureCallback:
    """
    List should be overridden by each subclass with appropriate values.
    """

    def __init__(self, module_name):
        self.called_from = module_name

    def get_extra_fields(self):
        """
        Error message might need to be overwritten when
        subclassed for various DAGs.
        'Error' is the default for FailureCallback, which indicates a pipeline has failed.
        """
        return {
            "validation_message": f"""
                Process {self.dag_run.dag_id} started {self.dag_run.execution_date}
                failed at task {self.task.task_id} in {self.called_from}.
                {f'Error: {self.formatted_exception}' if self.formatted_exception else ""}
            """,
        }

    def set_status(self):
        """
        The failure callback needs to set the dataset status,
        otherwise it will remain in the "Processing" state
        """
        data = self.get_extra_fields()
        logging.info("data:\n" + pformat(data))
        StatusChanger(
            self.uuid,
            self.auth_tok,
            status="error",
            fields_to_overwrite=data,
        ).update()

    def __call__(self, context):
        """
        This happens when the DAG to which the instance is attached actually
        encounters an error.
        """
        self.uuid = context.get("task_instance").xcom_pull(key="uuid")
        context["uuid"] = self.uuid
        self.auth_tok = get_auth_tok(**context)
        self.dag_run = context.get("dag_run")
        self.task = context.get("task")
        exception = context.get("exception")
        self.formatted_exception = formatted_exception(exception)

        self.set_status()
