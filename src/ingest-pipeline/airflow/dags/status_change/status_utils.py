from __future__ import annotations

import json
import traceback
import urllib.parse
from enum import Enum
from typing import Any

from requests import codes
from requests.exceptions import HTTPError

from airflow.providers.http.hooks.http import HttpHook


class EntityUpdateException(Exception):
    pass


class Statuses(str, Enum):
    # Dataset Hold and Deprecated are not currently in use but are valid for Entity API
    DATASET_DEPRECATED = "deprecated"
    DATASET_ERROR = "error"
    DATASET_HOLD = "hold"
    DATASET_INVALID = "invalid"
    DATASET_NEW = "new"
    DATASET_PROCESSING = "processing"
    DATASET_PUBLISHED = "published"
    DATASET_QA = "qa"
    DATASET_SUBMITTED = "submitted"
    PUBLICATION_ERROR = "error"
    PUBLICATION_HOLD = "hold"
    PUBLICATION_INVALID = "invalid"
    PUBLICATION_NEW = "new"
    PUBLICATION_PROCESSING = "processing"
    PUBLICATION_PUBLISHED = "published"
    PUBLICATION_QA = "qa"
    PUBLICATION_SUBMITTED = "submitted"
    UPLOAD_ERROR = "error"
    UPLOAD_INVALID = "invalid"
    UPLOAD_NEW = "new"
    UPLOAD_PROCESSING = "processing"
    UPLOAD_REORGANIZED = "reorganized"
    UPLOAD_SUBMITTED = "submitted"
    UPLOAD_VALID = "valid"


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


# This is simplified from pythonop_get_dataset_state in utils
def get_submission_context(token: str, uuid: str) -> dict[str, Any]:
    """
    uuid can also be a HuBMAP ID.
    """
    method = "GET"
    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
    }
    http_hook = HttpHook(method, http_conn_id="entity_api_connection")

    endpoint = f"entities/{uuid}"

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
    method = "GET"
    headers = {
        "authorization": f"Bearer {token}",
        "content-type": "application/json",
        "X-Hubmap-Application": "ingest-pipeline",
    }
    http_hook = HttpHook(method, http_conn_id="entity_api_connection")

    endpoint = f"entities/{uuid}"

    try:
        response = http_hook.run(
            endpoint, headers=headers, extra_options={"check_response": False}
        )
        response.raise_for_status()
        return response.json().get("hubmap_id")
    except HTTPError as e:
        print(f"ERROR: {e}")
        if e.response.status_code == codes.unauthorized:
            raise RuntimeError("entity database authorization was rejected?")
        print("benign error")
        return None


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


def get_abs_path(uuid: str, token: str) -> str:
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
    return response.json().get("path")


def get_organ(uuid: str, token: str) -> str:
    """
    Get ancestor organ for sample, dataset, or publication.
    """
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


def get_globus_url(uuid: str, token: str) -> str:
    """
    Return the Globus URL (default) for a dataset.
    URL format is https://app.globus.org/file-manager?origin_id=<id>&origin_path=<uuid | consortium|private/<group>/<uuid>>
    """
    path = get_abs_path(uuid, token)
    prefix = "https://app.globus.org/file-manager?"
    params = {}
    if "public" in path:
        params["origin_id"] = "af603d86-eab9-4eec-bb1d-9d26556741bb"
        params["origin_path"] = uuid
    else:
        params["origin_id"] = "24c2ee95-146d-4513-a1b3-ac0bfdb7856f"
        params["origin_path"] = path.replace("/hive/hubmap/data", "") + "/"
    return prefix + urllib.parse.urlencode(params)


def post_to_slack_notify(token: str, message: str, channel: str):
    http_hook = HttpHook("POST", http_conn_id="ingest_api_connection")
    payload = json.dumps({"message": message, "channel": channel})
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = http_hook.run("/notify", payload, headers)
    response.raise_for_status()
