import logging
from pprint import pformat

from status_change.callbacks.base import AirflowCallback
from status_change.status_manager import StatusChanger
from status_change.status_utils import formatted_exception


class FailureCallbackException(Exception):
    pass


class FailureCallback(AirflowCallback):
    """
    Usage:
        with HMDAG(
            ...
            "on_failure_callback": FailureCallback(__name__)
        )
    """

    def get_extra_fields(self):
        if not self.dag_run or not self.task:
            return {
                "validation_message": f"""Process failed in {self.called_from},
                but is missing either dag_run (value: '{self.dag_run}')
                or task (value: '{self.task}').
                {f'Error: {self.formatted_exception}' if self.formatted_exception else ""}
                """,
            }

        return {
            "validation_message": f"""
                Process {self.dag_run.dag_id} started {self.dag_run.execution_date}
                failed at task {self.task.task_id} in {self.called_from}.
                {f'Error: {self.formatted_exception}' if self.formatted_exception else ""}
            """,
        }

    def set_status(self):
        """
        FailureCallback needs to set the dataset status to "Error",
        otherwise it will remain in the "Processing" state.
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
        self.get_data(context)
        self.set_status()

    def get_data(self, context):
        super().get_data(context)
        exception = context.get("exception")
        self.formatted_exception = formatted_exception(exception)


class FailureCallbackPipeline(FailureCallback):
    """ """

    pass
