# import traceback

from status_change.status_manager import StatusChanger, StatusChangerExtras, Statuses
from utils import get_auth_tok


class FailureCallbackException(Exception):
    pass


class FailureCallback:
    # TODO: on hold until specifics of notifications are figured out
    # """
    # List(s) should be overwritten by each subclass with appropriate values.
    # """
    #
    # external_exceptions = []
    # internal_email_recipients = []

    def __init__(self, context, **kwargs):
        self.context = context
        self.uuid = self.context.get("task_instance").xcom_pull(key="uuid")
        self.auth_tok = get_auth_tok(**context)
        self.dag_run = self.context.get("dag_run")
        self.task = self.context.get("task")
        self.exception = self.context.get("exception")
        try:
            self.exception_name = type(self.exception).__name__
        except Exception:
            self.exception_name = None
        if kwargs:
            self.kwargs = kwargs

    def get_status(self):
        entity_type = self.context.get("task_instance").xcom_pull(key="entity_type")
        if entity_type == "Upload":
            return Statuses.UPLOAD_ERROR
        elif entity_type == "Dataset":
            return Statuses.DATASET_ERROR
        else:
            raise Exception(f"Unknown entity_type: {entity_type}, cannot change status.")

    def get_extra_fields(self):
        """
        Error message might need to be overwritten when
        subclassed for various DAGs.
        """
        return {
            "validation_message": f"Process {self.dag_run.dag_id} started {self.dag_run.execution_date} failed at task {self.task.task_id} with error {self.exception_name} {self.exception}",
        }

    def get_extra_options(self):
        return {}

    def set_status(self):
        status_changer_extras: StatusChangerExtras = {
            "extra_fields": self.get_extra_fields(),
            "extra_options": self.get_extra_options(),
        }
        StatusChanger(
            self.uuid, self.auth_tok, self.get_status(), status_changer_extras
        ).on_status_change()


# TODO: on hold until specifics of notifications are figured out
# def format_exception(self):
#     # traceback logic borrowed from https://stackoverflow.com/questions/51822029/get-exception-details-on-airflow-on-failure-callback-context
#     try:
#         formatted_exception = "".join(
#             traceback.TracebackException.from_exception(self.exception).format()
#         ).replace("\n", "<br>")
#     except Exception:
#         formatted_exception = None
#     return formatted_exception
