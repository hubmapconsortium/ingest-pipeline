import json
import logging
import re
import traceback
from enum import Enum
from functools import cached_property
from textwrap import dedent
from typing import Any, Optional
from urllib.parse import urlencode, urljoin

from requests import codes
from requests.exceptions import HTTPError

from airflow.models import DagRun
from airflow.providers.http.hooks.http import HttpHook

##############
# Exceptions #
##############


class EntityUpdateException(Exception):
    pass


#########
# Enums #
#########


class EntityType(str, Enum):
    DATASET = "dataset"
    PUBLICATION = "publication"
    UPLOAD = "upload"
    SAMPLE = "sample"


class Project(Enum):
    HUBMAP = ("hubmap", "HuBMAP")
    SENNET = ("sennet", "SenNet")


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
    def status_str(self) -> str:
        return self.value.split("_")[1]

    @property
    def entity_type_str(self) -> str:
        return self.value.split("_")[0]

    @property
    def titlecase(self) -> str:
        if self == Statuses.DATASET_QA:
            return "QA"
        return self.status_str.title()

    @staticmethod
    def valid_str(status: "str | Statuses") -> str:
        """
        Pass string version of status (any case,
        including entity_type prefix or not)
        or Statuses instance. Retrieve
        lower-case status string or raise if not valid.
        """
        if isinstance(status, Statuses):
            return status.status_str
        elif isinstance(status, str):
            status = status.lower()
            # Try with entity type prefix (e.g., "dataset_error")
            if "_" in status:
                try:
                    return Statuses(status).status_str
                except ValueError:
                    pass
            # Try without prefix (e.g., "error")
            for entity_type_map in ENTITY_STATUS_MAP.keys():
                if status in entity_type_map:
                    return status
        raise EntityUpdateException(f"Status {status} is not valid.")


# Needed some way to disambiguate statuses shared by datasets and uploads
ENTITY_STATUS_MAP = {
    EntityType.DATASET: {
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
    EntityType.PUBLICATION: {
        "error": Statuses.PUBLICATION_ERROR,
        "hold": Statuses.PUBLICATION_HOLD,
        "invalid": Statuses.PUBLICATION_INVALID,
        "new": Statuses.PUBLICATION_NEW,
        "processing": Statuses.PUBLICATION_PROCESSING,
        "published": Statuses.PUBLICATION_PUBLISHED,
        "qa": Statuses.PUBLICATION_QA,
        "submitted": Statuses.PUBLICATION_SUBMITTED,
    },
    EntityType.UPLOAD: {
        "error": Statuses.UPLOAD_ERROR,
        "invalid": Statuses.UPLOAD_INVALID,
        "new": Statuses.UPLOAD_NEW,
        "processing": Statuses.UPLOAD_PROCESSING,
        "reorganized": Statuses.UPLOAD_REORGANIZED,
        "submitted": Statuses.UPLOAD_SUBMITTED,
        "valid": Statuses.UPLOAD_VALID,
    },
}


################
# Base classes #
################


class MessageManager:

    def __init__(
        self,
        status: Statuses,
        uuid: str,
        token: str,
        messages: Optional[dict] = None,
        run_id: str = "",
    ):
        """Parent class for all messaging types (Slack, email,
        Data Ingest Board-specific metadata updates).

        Usage:
            SlackManager(
                    Statuses.<status>,
                    <uuid>,
                    <token>
                ).update()

        status -- status triggering this message
        uuid -- UUID of entity
        token -- Globus token
        messages -- "error_counts" and "error_dict" values, if present,
            are made accessible as class properties; subclasses may
            anticipate other values
        run_id -- Airflow run ID
        """
        self.uuid = uuid
        self.token = token
        self.status = status
        self.messages = messages if messages else {}
        self.run_id = run_id
        self.log_directory_path = get_log_directory_path(self.run_id)

    @property
    def is_valid_for_status(self) -> bool:
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    @cached_property
    def entity_data(self) -> dict:
        return get_submission_context(self.token, self.uuid)

    @cached_property
    def is_internal_error(self) -> bool:
        return is_internal_error(self.entity_data)

    @property
    def error_counts(self) -> str:
        if counts := self.messages.get("error_counts"):
            return "; ".join([f"{k}: {v}" for k, v in counts.items()])
        return ""

    @property
    def error_dict(self) -> dict:
        if error_dict := self.messages.get("error_dict"):
            return error_dict
        return {}


###########
# Configs #
###########

slack_channels = {
    "base": "C08V3TAP3GQ",  # testing-status-change
    "dataset_error": "C08V3TAP3GQ",
    "dataset_invalid": "C08V3TAP3GQ",
    "dataset_qa": "C099KMKJT26",  # dataset-qa-notifications
    "upload_error": "C08V3TAP3GQ",
    "upload_invalid": "C08V3TAP3GQ",
    "upload_reorganized": "C08V3TAP3GQ",
    "upload_priority_reorganized": "C08STFJTJKT",  # fasttrack-ingest
}

slack_channels_testing = {"base": "C08V3TAP3GQ"}


globus_dirs = {
    "hubmap": {
        "prod": {
            "public": "af603d86-eab9-4eec-bb1d-9d26556741bb",
            "protected": "24c2ee95-146d-4513-a1b3-ac0bfdb7856f",
        },
        "dev": {
            "public": "2b82f085-1d50-4c93-897e-cd79d77481ed",
            "protected": "ff1bd56e-2e65-4ec9-86fa-f79422884e96",
        },
        "path_replace_regex": r"/hive/hubmap.*/data",
    },
    "sennet": {
        "prod": {
            "public": "96b2b9e5-6915-4dbc-9ab5-173ad628902e",
            "protected": "45617036-f2cc-4320-8108-edf599290158",
        },
        "dev": {
            "public": "96b2b9e5-6915-4dbc-9ab5-173ad628902e",
            "protected": "b1571f8f-4ce5-4c81-9327-47bba11423ff",
        },
        "path_replace_regex": f"/codcc.*/data",
    },
}

####################
# Get data helpers #
####################


def get_project() -> Project:
    url = HttpHook.get_connection("ingest_api_connection").host
    if "hubmap" in str(url) or "hive" in str(url):
        return Project.HUBMAP
    return Project.SENNET


def get_entity_type(entity_data: dict) -> str:
    return entity_data.get("entity_type", "").lower()


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


# This is simplified from pythonop_get_dataset_state in utils
def get_submission_context(token: str, uuid: str) -> dict[str, Any]:
    """
    uuid can also be a HuBMAP/SenNet ID.
    """
    headers = get_headers(token)
    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

    endpoint = f"entities/{uuid}?exclude=direct_ancestors.files"

    try:
        response = http_hook.run(
            endpoint, headers=headers, extra_options={"check_response": False}
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as e:
        logging.error(e)
        if e.response.status_code == codes.unauthorized:
            raise RuntimeError(f"Authorization rejected for entity {uuid}") from e
        else:
            logging.error(f"HTTP error fetching entity {uuid}: {e}")
            raise


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


def get_organ(uuid: str, token: str, raise_on_exception: bool = False) -> str:
    """Get ancestor organ for sample, dataset, or publication.

    uuid -- UUID of entity
    token -- Globus token
    raise_on_exception -- optionally raise exception if organ not found
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
        logging.error(e)
        if raise_on_exception:
            raise
        return ""


def get_ancestors(uuid: str, token: str) -> dict:
    endpoint = f"/ancestors/{uuid}"
    headers = get_headers(token)
    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")
    response = http_hook.run(endpoint, headers)
    logging.info(f"""Response: {response.json()}""")
    return response.json()


def get_primary_dataset(entity_data: dict, token: str) -> Optional[str]:
    # Weed out multi-assay
    dag_provenance_list = entity_data.get("ingest_metadata", {}).get("dag_provenance_list", [])
    for dag in dag_provenance_list:
        if ".cwl" in dag.get("name", ""):
            # If it's been through a pipeline, then find ancestor dataset
            ancestors = get_ancestors(entity_data.get("uuid", ""), token)
            for ancestor in ancestors:
                if get_entity_type(entity_data) == EntityType.DATASET:
                    return ancestor.get("uuid")


def get_env() -> Optional[str]:
    from utils import find_matching_endpoint

    host = None
    for conn in ["ingest_api_connection", "entity_api_connection"]:
        if host := HttpHook.get_connection(conn).host:
            try:
                return find_matching_endpoint(host).lower()
            except Exception:
                continue
    logging.error(f"Could not determine env. Host: {host}.")


def get_entity_ingest_url(entity_data: dict) -> str:
    # ingest_url is generally in the vm00# format (at least for HuBMAP)
    # so some concatenation is necessary; this defaults to PROD HuBMAP URL
    url_end = "hubmapconsortium.org/"
    if get_project() == Project.SENNET:
        url_end = "sennetconsortium.org/"
    env = get_env()
    url_start = "https://ingest."
    if env not in ["prod", None]:
        url_start = f"https://ingest.{env}."
    base_url = urljoin(url_start + url_end, get_entity_type(entity_data))
    if not base_url.endswith("/"):
        base_url += "/"
    return urljoin(base_url, entity_data.get("uuid"))


def get_data_ingest_board_query_url(entity_data: dict) -> str:
    proj = get_project().value[0]
    env = get_env()
    if env == "prod":
        url = f"https://ingest.board.{proj}consortium.org/"
    else:
        url = f"https://ingest-board.{env}.{proj}consortium.org/"
    params = {"q": get_entity_id(entity_data)}
    if get_entity_type(entity_data) == EntityType.UPLOAD:
        params["entity_type"] = "uploads"
    return f"{url}?{urlencode(params)}"


def get_globus_url(uuid: str, token: str) -> str:
    """
    Return the Globus URL (default) for a dataset.
    URL format is https://app.globus.org/file-manager?origin_id=<id>&origin_path=<uuid | consortium|private/<group>/<uuid>>
    """
    path = get_abs_path(uuid, token)
    prefix = "https://app.globus.org/file-manager?"
    proj = get_project()
    project_dict = globus_dirs.get(proj.value[0]) or {}
    if not (env_dict := project_dict.get(get_env() or "", {})):
        return ""
    params = {}
    if "public" in path:
        params["origin_id"] = env_dict.get("public")
        params["origin_path"] = uuid
    else:
        regex = project_dict.get("path_replace_regex", "")
        params["origin_id"] = env_dict.get("protected")
        params["origin_path"] = re.sub(regex, "", path) + "/"
    return prefix + urlencode(params)


def get_run_id(run_id: str | DagRun) -> str:
    if isinstance(run_id, DagRun):
        return str(run_id.run_id)
    return str(run_id)


def get_log_directory_path(run_id: str) -> str:
    from utils import get_tmp_dir_path

    if not run_id:
        return ""
    return str(get_tmp_dir_path(run_id))


##################
# Classification #
##################

"""
Strings that should *only* occur in failure states
(e.g. not in the course of normal validation, where
"404" can appear in a real, external-facing error
message)
"""
internal_error_strs = ["EntityUpdateException", "Process failed", "Traceback", "Internal error"]


def is_internal_error(entity_data: dict) -> bool:
    status = entity_data.get("status", "").lower()
    if status == "error":
        return True
    elif validation_message := entity_data.get("validation_message"):
        for error_str in internal_error_strs:
            if error_str.lower() in validation_message.lower():
                return True
    return False


##############
# Formatting #
##############


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


def split_error_counts(error_message: str, no_bullets: bool = False) -> list[str]:
    if no_bullets:
        return [line for line in re.split("; | \\| ", error_message)]
    return [f"- {line}" for line in re.split("; | \\| ", error_message)]


def _enums_to_lowercase(data: Any) -> Any:
    """
    Lowercase all strings which appear as dictionary values.
    This modifies the passed data in place, rather than making
    a copy!
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, str):
                data[key] = value.lower()
            else:
                data[key] = _enums_to_lowercase(value)
        return data
    elif isinstance(data, list):
        return [_enums_to_lowercase(val) for val in data]
    else:
        return data


def format_multiline(string: str) -> str:
    return dedent(string.strip())


#############
# API calls #
#############


def post_to_slack_notify(token: str, message: str, channel: str):
    http_hook = HttpHook("POST", http_conn_id="ingest_api_connection")
    payload = json.dumps({"message": message, "channel": channel})
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = http_hook.run("/notify", payload, headers)
    response.raise_for_status()


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
