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

    def log(self):
        if not self.dag_run or not self.task:
            logging.error(
                f"""Process failed in {self.called_from},
                but is missing either dag_run (value: '{self.dag_run}')
                or task (value: '{self.task}').
                {f'Error: {self.formatted_exception}' if self.formatted_exception else ""}
                """
            )
            return
        logging.error(
            f"""
                Process failed: {self.dag_run.dag_id} started {self.dag_run.execution_date}
                failed at task {self.task.task_id} in {self.called_from}.
                {f'Error: {self.formatted_exception}' if self.formatted_exception else ""}
            """
        )

    def get_extra_fields(self):
        msg = f"Internal error. Process failed in {self.called_from}."
        if self.dag_run:
            msg += f" DAG run: {self.dag_run.dag_id}."
        if self.task:
            msg += f" Task ID: {self.task.task_id}."
        return {"error_message": msg}

    def set_status(self):
        """
        FailureCallback needs to set the dataset status to "Error",
        otherwise it will remain in the "Processing" state.
        """
        if not self.uuid:
            logging.info("No UUID passed, can't set status or send notifications.")
            return
        self.log()
        data = self.get_extra_fields()
        logging.info("data:\n" + pformat(data))
        StatusChanger(
            self.uuid,
            self.auth_tok,
            status="error",
            fields_to_overwrite=data,
            messages=self.messages,
        ).update()

    def __call__(self, context):
        """
        This happens when the DAG to which the instance is attached actually
        encounters an error.
        """
        try:
            if context.get("dag_run").conf.get("dryrun"):  # type: ignore
                return
        except Exception as e:
            logging.info(e)
            return
        self.context = context
        self.get_data()
        if self.uuid:
            self.set_status()
        else:
            logging.info(f"No uuid sent with context, can't update status. Context:")
            logging.info(pformat(self.context))
            logging.error("Cause of failure:")
            logging.error(self.formatted_exception)

    def get_data(self):
        super().get_data()
        exception = self.context.get("exception")
        self.formatted_exception = formatted_exception(exception)
        self.messages["processing_pipeline"] = self.check_for_pipeline()

    def check_for_pipeline(self) -> str:
        """
        Some messaging rules depend on the presence of information
        about pipelines. If we have it, add it to messages.
        """
        pipeline = self.context.get("pipeline_shorthand") or self.context.get("pipeline_name", "")
        if not pipeline:
            try:
                pipeline = self.context["ti"].xcom_pull(key="pipeline_name")
            except Exception:
                pass
        return pipeline
