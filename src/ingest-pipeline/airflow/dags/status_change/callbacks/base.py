from abc import ABC

from status_change.status_utils import get_submission_context
from utils import get_auth_tok


class AirflowCallback(ABC):
    """
    May be used as an argument for DAG params that take a callable
    (e.g. on_failure_callback, on_success_callback).

    Usage:
        with HMDAG(
            ...
            <param>: <AirflowCallbackSubclass>(__name__)
        )
    """

    def __init__(self, module_name: str):
        self.called_from = module_name

    def __call__(self, context: dict):
        """
        Likely implementation:
            self.get_data(context)
            # Do something
        """
        raise NotImplementedError

    def get_data(self, context: dict):
        self.uuid = context["task_instance"].xcom_pull(key="uuid")
        context["uuid"] = self.uuid
        self.auth_tok = get_auth_tok(**context)
        self.dag_run = context.get("dag_run")
        self.task = context.get("task")
        self.entity_data = get_submission_context(self.auth_tok, self.uuid)
        self.entity_type = self.entity_data.get("entity_type", "").lower()
