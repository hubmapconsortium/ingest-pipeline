import json
import logging
from pprint import pprint

from airflow.providers.http.hooks.http import HttpHook


def set_entity_api_status(uuid: str, token: str, data: dict, extra_options: dict | None = None):
    if not data:
        raise Exception(f"No data provided to update status for dataset {uuid}")
    endpoint = f"/entities/{uuid}"
    headers = {
        "authorization": "Bearer " + token,
        "X-Hubmap-Application": "ingest-pipeline",
        "content-type": "application/json",
    }
    if extra_options is None:
        extra_options = {}
    http_conn_id = "entity_api_connection"
    http_hook = HttpHook("PUT", http_conn_id=http_conn_id)
    logging.info(
        f"""
        data:
        {pprint(data)}
        """
    )
    try:
        response = http_hook.run(
            endpoint,
            json.dumps(data),
            headers,
            extra_options,
        )
        response.check_response()
    except Exception as e:
        logging.info(f"Encountered error, status not set. Error: {e}")
        raise
    return response
