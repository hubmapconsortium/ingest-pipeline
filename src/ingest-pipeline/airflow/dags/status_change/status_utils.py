from __future__ import annotations

import json
import logging
import traceback
from enum import Enum
from typing import Any, Optional
from urllib.parse import urlencode

from requests import codes
from requests.exceptions import HTTPError

from airflow.providers.http.hooks.http import HttpHook


class EntityUpdateException(Exception):
    pass


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

    @staticmethod
    def get_status_str(status: Statuses):
        return status.split("_")[1]

    @staticmethod
    def get_entity_type_str(status: Statuses):
        return status.split("_")[0]


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


# This is simplified from pythonop_get_dataset_state in utils
def get_submission_context(token: str, uuid: str) -> dict[str, Any]:
    """
    uuid can also be a HuBMAP ID.
    """
    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
    }
    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

    endpoint = f"entities/{uuid}?exclude=direct_ancestors.files"

    try:
        response = http_hook.run(
            endpoint, headers=headers, extra_options={"check_response": False}
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as e:
        print(f"ERROR: {e}")
        if e.response.status_code == codes.unauthorized:
            raise RuntimeError("entity database authorization was rejected?")
        print("benign error")
        return {}


def get_hubmap_id_from_uuid(token: str, uuid: str) -> str | None:
    return get_submission_context(token, uuid).get("hubmap_id")


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
    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
    }
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


def post_to_slack_notify(token: str, message: str, channel: str):
    http_hook = HttpHook("POST", http_conn_id="ingest_api_connection")
    payload = json.dumps({"message": message, "channel": channel})
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = http_hook.run("/notify", payload, headers)
    response.raise_for_status()


def get_ancestors(uuid: str, token: str) -> dict:
    endpoint = f"/ancestors/{uuid}"
    headers = {
        "authorization": "Bearer " + token,
        "X-Hubmap-Application": "ingest-pipeline",
        "content-type": "application/json",
    }
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
                if ancestor.get("entity_type", "").lower() == "dataset":
                    return ancestor.get("uuid")


def put_request_to_entity_api(
    uuid: str, token: str, update_fields: dict, params: dict = {}
) -> dict:
    endpoint = f"/entities/{uuid}"
    if encoded_params := urlencode(params):
        endpoint += f"?{encoded_params}"
    headers = {
        "authorization": "Bearer " + token,
        "X-Hubmap-Application": "ingest-pipeline",
        "content-type": "application/json",
    }
    http_hook = HttpHook("PUT", http_conn_id="entity_api_connection")
    response = http_hook.run(endpoint, json.dumps(update_fields), headers)
    logging.info(f"""Response: {response.json()}""")
    return response.json()


def get_env() -> Optional[str]:
    from utils import find_matching_endpoint

    if host := HttpHook.get_connection("entity_api_connection").host:
        return find_matching_endpoint(host)
    logging.error(f"Could not determine env. Host: {host}.")
