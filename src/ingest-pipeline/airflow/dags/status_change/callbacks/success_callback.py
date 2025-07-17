from urllib.parse import urljoin

from status_change.status_utils import get_submission_context, post_to_slack_notify

from airflow.configuration import conf as airflow_conf

from .base import AirflowCallback


class SuccessCallback(AirflowCallback):
    """
    Usage:
        with HMDAG(
            ...
            "on_success_callback": SuccessCallback(__name__)
        )
    """

    def __call__(self, context):
        """
        This happens when the DAG to which the instance is attached succeeds.
        """
        self.get_data(context)
        self.send_slack_message()

    def get_data(self, context):
        super().get_data(context)
        entity_data = get_submission_context(self.auth_tok, self.uuid)
        self.status = entity_data.get("status")
        self.entity_type = entity_data.get("entity_type", "")
        self.log_url = self.get_log_url(context)
        self.send_slack_message()

    def get_log_url(self, context):
        base_url = airflow_conf.as_dict().get("webserver", {}).get("base_url")
        if not type(base_url) is str:
            base_url = None
        dag = context.get("dag")
        return urljoin(base_url, f"?dag_id={dag.dag_id}") if base_url and dag else None

    def send_slack_message(self):
        msg = f"Process {self.called_from} has succeeded for {self.entity_type} {self.uuid}. Status is: {self.status}."
        if self.log_url:
            msg += f" Airflow run: {self.log_url}"
        # TODO: get channel
        channel = ""
        post_to_slack_notify(self.auth_tok, msg, channel)
