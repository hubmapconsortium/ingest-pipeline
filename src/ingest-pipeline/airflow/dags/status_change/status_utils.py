from __future__ import annotations

import json
import logging
import re
import traceback
from enum import Enum
from typing import Any, Optional, Union
from urllib.parse import urlencode, urljoin

import requests

# make utils available to other files in dir to prevent circular import
from utils import (
    decrypt_tok,
    env_appropriate_slack_channel,
    find_matching_endpoint,
    get_auth_tok,
    get_env,
)
from utils import get_submission_context as base_get_submission_context
from utils import (
    get_tmp_dir_path,
    get_uuid_for_error,
)
from utils import send_email as main_send_email

from airflow.models import DagRun
from airflow.providers.http.hooks.http import HttpHook


class EntityUpdateException(Exception):
    pass


"""
Strings that should *only* occur in failure states
(e.g. not in the course of normal validation, where
"404" can appear in a real, external-facing error
message)
"""
internal_error_strs = ["EntityUpdateException", "Process failed", "Traceback", "Internal error"]


class Statuses(str, Enum):
    # Dataset Hold and Deprecated are not currently in use but are valid for Entity API
    DATASET_DEPRECATED = "dataset_deprecated"
    DATASET_ERROR = "dataset_error"
    DATASET_HOLD = "dataset_hold"
    DATASET_INVALID = "dataset_invalid"
    DATASET_NEW = "dataset_new"
    DATASET_PROCESSING = "dataset_processing"
    DATASET_PUBLISHED = "dataset_published"
    DATASET_QA = "dataset_qa"
    DATASET_SUBMITTED = "dataset_submitted"
    PUBLICATION_ERROR = "publication_error"
    PUBLICATION_HOLD = "publication_hold"
    PUBLICATION_INVALID = "publication_invalid"
    PUBLICATION_NEW = "publication_new"
    PUBLICATION_PROCESSING = "publication_processing"
    PUBLICATION_PUBLISHED = "publication_published"
    PUBLICATION_QA = "publication_qa"
    PUBLICATION_SUBMITTED = "publication_submitted"
    UPLOAD_ERROR = "upload_error"
    UPLOAD_INVALID = "upload_invalid"
    UPLOAD_NEW = "upload_new"
    UPLOAD_PROCESSING = "upload_processing"
    UPLOAD_REORGANIZED = "upload_reorganized"
    UPLOAD_SUBMITTED = "upload_submitted"
    UPLOAD_VALID = "upload_valid"

    @property
    def status_str(self):
        return self.value.split("_")[1]

    @property
    def entity_type_str(self):
        return self.value.split("_")[0]

    @property
    def titlecase(self) -> str:
        if self == Statuses.DATASET_QA:
            return "QA"
        return self.status_str.title()

    @staticmethod
    def valid_str(status: Union[str, Statuses]) -> str:
        """
        Pass string version of status (any case,
        including entity_type prefix or not)
        or Statuses instance. Retrieve
        lower-case status string or raise if not valid.
        """
        if type(status) is str:
            status = status.lower()
            if "_" in status:
                membership = [member for member in Statuses if status == member.value]
                if len(membership) == 1:
                    return membership[0].status_str
            else:
                for entity_type in ENTITY_STATUS_MAP.keys():
                    for status_str in ENTITY_STATUS_MAP[entity_type].keys():
                        if status == status_str:
                            return status
        elif isinstance(status, Statuses):
            return status.status_str
        raise EntityUpdateException(f"Status {status} is not valid.")


# Needed some way to disambiguate statuses shared by datasets and uploads
ENTITY_STATUS_MAP = {
    "dataset": {
        "deprecated": Statuses.DATASET_DEPRECATED,
        "error": Statuses.DATASET_ERROR,
        "hold": Statuses.DATASET_HOLD,
        "invalid": Statuses.DATASET_INVALID,
        "new": Statuses.DATASET_NEW,
        "processing": Statuses.DATASET_PROCESSING,
        "published": Statuses.DATASET_PUBLISHED,
        "qa": Statuses.DATASET_QA,
        "submitted": Statuses.DATASET_SUBMITTED,
    },
    "publication": {
        "error": Statuses.PUBLICATION_ERROR,
        "hold": Statuses.PUBLICATION_HOLD,
        "invalid": Statuses.PUBLICATION_INVALID,
        "new": Statuses.PUBLICATION_NEW,
        "processing": Statuses.PUBLICATION_PROCESSING,
        "published": Statuses.PUBLICATION_PUBLISHED,
        "qa": Statuses.PUBLICATION_QA,
        "submitted": Statuses.PUBLICATION_SUBMITTED,
    },
    "upload": {
        "error": Statuses.UPLOAD_ERROR,
        "invalid": Statuses.UPLOAD_INVALID,
        "new": Statuses.UPLOAD_NEW,
        "processing": Statuses.UPLOAD_PROCESSING,
        "reorganized": Statuses.UPLOAD_REORGANIZED,
        "submitted": Statuses.UPLOAD_SUBMITTED,
        "valid": Statuses.UPLOAD_VALID,
    },
}


def get_status_enum(entity_type: str, status: Statuses | str, uuid: str) -> Statuses:
    if type(status) is str:
        try:
            print(f"Looking for {entity_type.lower()}_{status.lower()} in ENTITY_STATUS_MAP.")
            status = ENTITY_STATUS_MAP[entity_type.lower()][status.lower()]
            print(f"Found {status}.")
        except KeyError:
            raise EntityUpdateException(
                f"""
                Could not retrieve status for {uuid}.
                Check that status is valid for entity type.
                Status not changed.
                """
            )
    assert type(status) is Statuses
    return status


class MessageManager:

    def __init__(
        self,
        status: Statuses | str,
        uuid: str,
        token: str,
        messages: Optional[dict] = None,
        *args,
        **kwargs,
    ):
        self.uuid = uuid
        self.token = token
        self.messages = messages if messages else {}
        self.args = args
        self.kwargs = kwargs
        self.entity_data = get_submission_context(self.token, self.uuid)
        self.status = self.get_status(status)
        self.is_internal_error = is_internal_error(self.entity_data)
        self.log_directory_path = log_directory_path(self.run_id)

    @property
    def is_valid_for_status(self) -> bool:
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def get_status(self, status: Statuses | str) -> Statuses:
        return get_status_enum(self.entity_data.get("entity_type", ""), status, self.uuid)

    @property
    def error_counts(self) -> str:
        if counts := self.messages.get("error_counts"):
            return "; ".join([f"{k}: {v}" for k, v in counts.items()])
        return ""

    @property
    def error_dict(self) -> dict:
        return self.messages.get("error_dict", {})

    @property
    def processing_pipeline(self) -> str:
        return self.messages.get("processing_pipeline", "")

    @property
    def run_id(self) -> str:
        run_id = self.messages.get("run_id", "")
        return get_run_id(run_id)

    @property
    def derived(self) -> bool:
        if get_is_derived(self.entity_data):
            return True
        return False


"""
Use utils.get_env_appropriate_slack_channel in order to
automatically switch to utils.DEFAULT_SLACK_TEST_CHANNEL
when not on PROD.
"""
base_slack_channel = "C08V3TAP3GQ"  # testing-status-change
slack_channels = {
    "base": base_slack_channel,
    "dataset_error": base_slack_channel,
    "dataset_error_processing": base_slack_channel,
    "dataset_invalid": base_slack_channel,
    "dataset_new_derived": base_slack_channel,
    "dataset_qa": "C099KMKJT26",  # dataset-qa-notifications
    "dataset_qa_derived": base_slack_channel,
    "upload_error": base_slack_channel,
    "upload_invalid": base_slack_channel,
    "upload_reorganized": base_slack_channel,
    "upload_priority_reorganized": "C08STFJTJKT",  # fasttrack-ingest
}


class Project(Enum):
    HUBMAP = ("hubmap", "HuBMAP")
    SENNET = ("sennet", "SenNet")


def get_project() -> Project:
    url = HttpHook.get_connection("ingest_api_connection").host
    if "hubmap" in str(url) or "hive" in str(url):
        return Project.HUBMAP
    return Project.SENNET


def get_entity_id(entity_data: dict) -> str:
    if get_project() == Project.HUBMAP:
        entity_id = entity_data.get("hubmap_id", "")
    else:
        entity_id = entity_data.get("sennet_id", "")
    return entity_id


def get_headers(token: str) -> dict:
    proj = get_project().value[0].title()
    return {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
        f"X-{proj}-Application": "ingest-pipeline",
    }


def get_submission_context(token: str, uuid: str) -> dict[str, Any]:
    return base_get_submission_context(token, uuid, get_headers(token))


def formatted_exception(exception):
    """
    traceback logic from
    https://stackoverflow.com/questions/51822029/get-exception-details-on-airflow-on-failure-callback-context
    """
    if not (
        formatted_exception := "".join(
            traceback.TracebackException.from_exception(exception).format()
        ).replace("\n", "<br>")
    ):
        return None
    return formatted_exception


def get_abs_path(uuid: str, token: str, escaped: bool = False) -> str:
    http_hook = HttpHook("GET", http_conn_id="ingest_api_connection")
    headers = get_headers(token)
    response = http_hook.run(
        endpoint=f"datasets/{uuid}/file-system-abs-path",
        headers=headers,
    )
    abs_path = response.json().get("path")
    if escaped:
        return abs_path.replace(" ", "\\ ")
    return abs_path


def get_organ(uuid: str, token: str) -> str:
    """
    Get ancestor organ for sample, dataset, or publication.
    """
    if not uuid:
        return ""
    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")
    response = http_hook.run(
        f"/entities/{uuid}/ancestor-organs", headers={"Authorization": "Bearer " + token}
    )
    try:
        response.raise_for_status()
        return response.json()[0].get("organ")
    except Exception as e:
        print(e)
        return ""


def get_ancestors(uuid: str, token: str) -> dict:
    endpoint = f"/ancestors/{uuid}"
    headers = get_headers(token)
    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")
    response = http_hook.run(endpoint, headers=headers)
    return response.json()


def get_primary_dataset(entity_data: dict, token: str) -> str | None:
    if not "central process" in entity_data.get("creation_action", "").lower():
        return
    ancestors = get_ancestors(entity_data.get("uuid", ""), token)
    for ancestor in ancestors:
        if ancestor.get("entity_type", "").lower() == "dataset":
            return ancestor.get("uuid")


def put_request_to_entity_api(
    uuid: str,
    token: str,
    update_fields: dict,
    params: dict = {},
) -> dict:
    endpoint = f"/entities/{uuid}"
    if encoded_params := urlencode(params):
        endpoint += f"?{encoded_params}"
    headers = get_headers(token)
    http_hook = HttpHook("PUT", http_conn_id="entity_api_connection")
    response = http_hook.run(endpoint, json.dumps(update_fields), headers)
    logging.info(f"""Response: {response.json()}""")
    return response.json()


def is_internal_error(entity_data: dict) -> bool:
    status = entity_data.get("status", "").lower()
    if status == "error":
        return True
    elif validation_message := entity_data.get("validation_message"):
        for error_str in internal_error_strs:
            if error_str.lower() in validation_message.lower():
                return True
    return False


def get_entity_ingest_url(entity_data: dict) -> str:
    # ingest_url is generally in the vm00# format (at least for HuBMAP)
    # so some concatenation is necessary; this defaults to PROD HuBMAP URL
    url_end = "hubmapconsortium.org/"
    if get_project() == Project.SENNET:
        url_end = "sennetconsortium.org/"
    env = get_env()
    url_start = "https://ingest."
    if env != "prod":
        url_start = f"https://ingest.{env}."
    entity_type = entity_data.get("entity_type", "").lower()
    base_url = urljoin(url_start + url_end, entity_type)
    if not base_url.endswith("/"):
        base_url += "/"
    return urljoin(base_url, entity_data.get("uuid"))


def get_data_ingest_board_query_url(entity_data: dict) -> str:

    proj = get_project().value[0]
    env = None
    for conn in ["ingest_api_connection", "entity_api_connection"]:
        if host := HttpHook.get_connection(conn).host:
            try:
                env = find_matching_endpoint(host).lower()
            except Exception:
                continue
    if not env:
        raise Exception(f"Could not determine env.")
    if env.lower() == "prod":
        url = f"https://ingest.board.{proj}consortium.org/"
    else:
        url = f"https://ingest-board.{env.lower()}.{proj}consortium.org/"
    entity_id = get_entity_id(entity_data)
    params = {"q": entity_id}
    if entity_data.get("entity_type", "").lower() == "upload":
        params["entity_type"] = "uploads"
    return f"{url}?{urlencode(params)}"


def get_globus_url(entity_data: dict, token: str) -> Optional[str]:
    """
    Return the Globus URL (default) for an entity.
    """
    url_end = "hubmapconsortium.org/"
    if get_project() == Project.SENNET:
        url_end = "sennetconsortium.org/"
    env = get_env()
    url_start = "https://entity.api."
    if env != "prod":
        url_start = f"https://entity-api.{env}."
    entity_type = entity_data.get("entity_type", "").lower()
    prefix = f"{url_start}{url_end}/entities/"
    base_url = urljoin(prefix, entity_type)
    if not base_url.endswith("/"):
        base_url += "/"
    path = f"globus-url/{entity_data['uuid']}"
    response = requests.get(urljoin(base_url, path), headers=get_headers(token))
    try:
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Could not retrieve Globus URL. Error: {e}")
        return
    return response.text


def get_run_id(run_id) -> str:
    if isinstance(run_id, DagRun):
        return run_id.run_id if type(run_id.run_id) is str else ""
    if type(run_id) is str:
        return run_id
    return ""


def log_directory_path(run_id: str) -> str:

    if not run_id:
        return ""
    return str(get_tmp_dir_path(run_id))


def split_error_counts(error_message: str, no_bullets: bool = False) -> list[str]:
    if no_bullets:
        return [line for line in re.split("; | \\| ", error_message)]
    return [f"- {line}" for line in re.split("; | \\| ", error_message)]


def get_is_derived(entity_data: dict) -> bool:
    if not entity_data.get("entity_type", "").lower() == "dataset":
        return False
    if entity_data.get("creation_action", "") == "Central Process":
        return True
    return False
