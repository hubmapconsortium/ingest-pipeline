import logging
from abc import ABC
from typing import Callable

from status_change.status_utils import get_run_id, get_submission_context
from utils import decrypt_tok, get_auth_tok, get_uuid_for_error


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
            self.context = context
            self.get_data(context)
            # Do something
        """
        self.context = context
        raise NotImplementedError

    def get_data(self):
        # Try several methods of figuring out UUID
        if not (
            uuid := (
                self.dataset_uuid_callable(**self.context) if self.dataset_uuid_callable else ""
            )
        ):
            if not (uuid := self.context["task_instance"].xcom_pull(key="uuid")):
                if not (uuid := get_uuid_for_error(**self.context)):
                    logging.error(
                        "Could not determine UUID, no status change or messaging actions will be taken."
                    )
                    self.uuid = ""
                    return
        self.uuid = uuid
        self.context["uuid"] = uuid
        try:
            self.auth_tok = get_auth_tok(**self.context)
        except KeyError:
            self.auth_tok = self.alt_get_auth_tok()
        self.dag_run = self.context.get("dag_run")
        self.task = self.context.get("task")
        self.entity_data = get_submission_context(self.auth_tok, self.uuid)
        self.entity_type = self.entity_data.get("entity_type", "").lower()
        self.messages = {"run_id": get_run_id(self.dag_run)}

    def alt_get_auth_tok(self):
        crypt_auth_tok = self.context["params"]["crypt_auth_tok"]
        auth_tok = "".join(
            e for e in decrypt_tok(crypt_auth_tok.encode()) if e.isalnum()
        )  # strip out non-alnum characters
        return auth_tok
