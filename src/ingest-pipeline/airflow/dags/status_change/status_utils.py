from requests import codes
from requests.exceptions import HTTPError

from airflow.hooks.http_hook import HttpHook


# This is simplified from pythonop_get_dataset_state in utils
def get_submission_context(token: str, uuid: str) -> dict:
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
        submission_data = response.json()
        return submission_data
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
    # http_hook = HttpHook(method, http_conn_id="entity_api_connection")
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
