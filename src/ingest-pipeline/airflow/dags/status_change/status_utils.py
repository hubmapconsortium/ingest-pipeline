from __future__ import annotations

import traceback
from enum import Enum
from typing import Any, Dict, Union

import asana

# from asana.models.custom_field_setting_response_array import (
#     CustomFieldSettingResponseArray,
# )
from requests import codes
from requests.exceptions import HTTPError

from airflow.hooks.http_hook import HttpHook


# This is simplified from pythonop_get_dataset_state in utils
def get_submission_context(token: str, uuid: str) -> Dict[str, Any]:
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
        else:
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
        else:
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


def compile_status_enum(status: str, entity_type: str, uuid: str) -> Statuses:
    """
    If status is passed as a string, get the entity type and match
    to correct entry in ENTITY_STATUS_MAP. Also check current status,
    because ingest-pipeline will error if you try to set the same status
    over the existing status.
    Potential TODO: could stop any operation involving "Published"
    statuses at this stage.
    """
    try:
        entity_status = ENTITY_STATUS_MAP[entity_type.lower()][status.lower()]
        return entity_status
    except KeyError:
        raise Exception(
            f"""
                Could not retrieve status for {uuid}.
                Check that status is valid for entity type.
                Status not changed.
            """
        )


# # TODO: CONVERT BACK TO ASANA 3.2.2
# def get_asana_fields(asana_token: str, project: str) -> None:
#     """
#     Helper function to print data if you need to update the custom fields;
#     needs to be called manually. Assumes a particular shape/organization
#     of the Asana board.
#     From this directory:
#     python -c "import status_utils; status_utils.get_asana_fields('<asana_token>' '<project_gid>')"
#     """
#     configuration = asana.Configuration()
#     configuration.access_token = asana_token
#     client = asana.ApiClient(configuration)
#     api_instance = asana.CustomFieldSettingsApi(client)
#     try:
#         api_response = api_instance.get_custom_field_settings_for_project(project)
#         assert api_response is not None
#         assert isinstance(api_response, CustomFieldSettingResponseArray)
#         assert api_response.data is not None
#     except ApiException as e:
#         print("Exception when calling get_custom_field_settings_for_project: %s\n" % e)
#         return
#     for field in api_response.data:
#         custom_field = field.custom_field
#         if custom_field.name == "HuBMAP ID":
#             print(f"HUBMAP_ID_FIELD_GID = '{custom_field.gid}'")
#         elif custom_field.name == "Process Stage":
#             print(f"PROCESS_STAGE_FIELD_GID = '{custom_field.gid}'")
#             process_enums = {}
#             for option in custom_field.enum_options:
#                 process_enums[option.name] = option.gid
#             print(f"PROCESS_STAGE_GIDS = {process_enums}")
#         # TODO: Below fields do not exist in Asana yet
#         elif custom_field.name == "Parent HuBMAP ID":
#             print(f"PARENT_FIELD_GID = '{custom_field.gid}'")
#         elif custom_field.name == "Entity Type":
#             print(f"ENTITY_TYPE_FIELD_GID = '{custom_field.gid}'")
#             entity_enums = {}
#             for option in custom_field.enum_options:
#                 entity_enums[option.name] = option.gid
#             print(f"ENTITY_TYPE_GIDS = {entity_enums}")
