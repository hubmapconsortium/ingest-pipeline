from abc import ABC
from typing import Callable

from status_change.status_utils import get_submission_context
from utils import decrypt_tok, get_auth_tok


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

    def __init__(self, module_name: str, dataset_uuid_callable: Callable | None = None):
        self.called_from = module_name
        self.dataset_uuid_callable = dataset_uuid_callable

    def __call__(self, context: dict):
        """
        Likely implementation:
            self.get_data(context)
            # Do something
        """
        raise NotImplementedError

    def get_data(self, context: dict):

        if self.dataset_uuid_callable:
            self.uuid = self.dataset_uuid_callable(**context)
        else:
            self.uuid = context["task_instance"].xcom_pull(key="uuid")
        context["uuid"] = self.uuid
        try:
            self.auth_tok = get_auth_tok(**context)
        except KeyError:
            self.auth_tok = self.alt_get_auth_tok(**context)
        self.dag_run = context.get("dag_run")
        self.task = context.get("task")
        self.entity_data = get_submission_context(self.auth_tok, self.uuid)
        self.entity_type = self.entity_data.get("entity_type", "").lower()

    def alt_get_auth_tok(self, context):
        crypt_auth_tok = context["params"]["crypt_auth_tok"]
        auth_tok = "".join(
            e for e in decrypt_tok(crypt_auth_tok.encode()) if e.isalnum()
        )  # strip out non-alnum characters
        return auth_tok
