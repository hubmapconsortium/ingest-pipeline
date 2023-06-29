from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import datetime, timedelta

import asana
import requests
from requests.exceptions import HTTPError
from status_change.status_manager import Statuses, UpdateAsana
from utils import (
    HMDAG,
    encrypt_tok,
    get_auth_tok,
    get_queue_resource,
    pythonop_get_dataset_state,
)

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

# Following are defaults which can be overridden later on
default_args = {
    "owner": "hubmap",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 28),
    "email": ["joel.welling@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "xcom_push": True,
    "queue": get_queue_resource("update_asana"),
}

with HMDAG(
    "update_asana",
    schedule_interval="@daily",
    is_paused_upon_creation=False,
    default_args=default_args,
) as dag:

    def get_asana_tasks(**kwargs) -> dict[str, dict[str, str]]:
        asana_dict = {}
        client = asana.Client.access_token(os.environ["ASANA_API_KEY"])
        # Draft for restricting to only non-Publishing status tasks; requires addition of field in Asana to filter them out
        # Remove UpdateAsana instance from compare_results and reference this one instead if implemented
        """
        asana_helper = UpdateAsana(
            uuid=None,
            token=get_auth_tok(**kwargs),
            status=None,
            workspace_id=os.environ["WORKSPACE_ID"],
            project_id=os.environ["PROJECT_ID"],
        )
        asana_status_map = asana_helper.asana_status_map
        asana_process_stage_gid = asana_helper.process_stage_field_gid
        kwargs["ti"].xcom_push(key="client", value=asana_status_map)
        response = list(
            client.tasks.search_tasks_for_workspace(
                os.environ["WORKSPACE_ID"],
                {
                    "projects_any": os.environ["PROJECT_ID"],
                    # This is where a field applied to active tasks would be needed for filtering
                    f"custom_fields.{asana_process_stage_gid}.contains": asana_helper.process_stage_gids[
                        "Publishing"
                    ],
                    "opt_fields": ["gid", "custom_fields"],
                },
            )
        )
        """
        response = list(
            client.tasks.get_tasks_for_project(
                os.environ["PROJECT_ID"], opt_fields=["custom_fields"]
            )
        )
        for task in response:
            try:
                hubmap_id = list(
                    field["text_value"]
                    for field in task["custom_fields"]
                    if field["name"] == "HuBMAP ID"
                )[0]
                if hubmap_id is None:
                    logging.info(f"No HuBMAP ID found for task; skipping. Task info: {task}.")
                    continue
                asana_status = list(
                    field["enum_value"]
                    for field in task["custom_fields"]
                    if field["name"] == "Process Stage"
                )[0]
                asana_dict[hubmap_id] = {"asana_status": asana_status, "task_id": task["gid"]}
            except Exception:
                logging.info(
                    f"Error compiling information (HuBMAP ID, Asana Process Stage, and task GID) for task. Task info retrieved from Asana API follows: {task}"
                )
        return asana_dict

    t_get_asana_tasks = PythonOperator(
        task_id="get_asana_tasks",
        python_callable=get_asana_tasks,
        provide_context=True,
    )

    def api_query(hubmap_id: str, **kwargs) -> dict:
        headers = {
            "authorization": f"Bearer {get_auth_tok(**kwargs)}",
            "content-type": "application/json",
            "X-Hubmap-Application": "ingest-pipeline",
        }
        http_hook = HttpHook("GET", http_conn_id="entity_api_connection")
        endpoint = f"entities/{hubmap_id}"
        try:
            response = http_hook.run(
                endpoint, headers=headers, extra_options={"check_response": True}
            )
            return response
        except HTTPError as e:
            print(f"ERROR: {e}")
            if e.response.status_code == requests.codes.unauthorized:
                raise RuntimeError("entity database authorization was rejected?")
            else:
                print("benign error")
                return {}

    def query_entity_api(**kwargs) -> defaultdict[str, dict[str, str]]:
        asana_dict = kwargs["ti"].xcom_pull(task_ids="get_asana_tasks").copy()
        entity_api_dict = defaultdict(dict)
        for hubmap_id in asana_dict.keys():
            entity_api_record = api_query(hubmap_id, **kwargs)
            if not entity_api_record:
                logging.info(f"Error retrieving record for {hubmap_id} from Entity API.")
                continue
            entity_api_dict["entity_type"][entity_api_record["hubmap_id"]] = entity_api_record[
                "status"
            ]
        return entity_api_dict

    t_query_entity_api = PythonOperator(
        task_id="query_entity_api",
        python_callable=query_entity_api,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            ),
        },
    )

    def compare_results(**kwargs) -> dict[str, list | dict]:
        asana_status_map = UpdateAsana(
            uuid=None,
            token=get_auth_tok(**kwargs),
            status=None,
            workspace_id=os.environ["WORKSPACE_ID"],
            project_id=os.environ["PROJECT_ID"],
        ).asana_status_map
        asana_dict = kwargs["ti"].xcom_pull(task_ids="get_asana_tasks").copy()
        entity_api_dict = kwargs["ti"].xcom_pull(task_ids="query_entity_api").copy()
        errors = {"missing_key": [], "missing_status": [], "mismatch": {}}
        for entity_type in asana_dict:
            for key, value in entity_type.items():
                try:
                    entity_api_status = entity_api_dict["key"].get("status")
                except KeyError:
                    errors["missing_key"].append(key)
                    continue
                if entity_api_status is None:
                    errors["missing_status"].append(key)
                    continue
                mapped_entity_api_status = asana_status_map[
                    Statuses[f"{entity_type.upper()}_{entity_api_status.upper()}"]
                ]
                if value["asana_status"] == mapped_entity_api_status:
                    continue
                else:
                    errors["mismatch"][key] = {
                        "status": mapped_entity_api_status,
                        "task_id": value["task_id"],
                    }
        return errors

    t_compare_results = PythonOperator(
        task_id="compare_results",
        python_callable=compare_results,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            ),
        },
    )

    def update_asana_if_needed(**kwargs):
        errors = kwargs["ti"].xcom_pull(task_ids="compare_results").copy()
        client = asana.Client.access_token(os.environ["ASANA_API_KEY"])
        asana_hubmap_id_gid = kwargs["ti"].xcom_pull(task_ids="get_asana_tasks").copy()["gid"]
        if not any([errors["missing_key"], errors["missing_status"], errors["mismatch"]]):
            logging.info("No errors or discrepancies found, no Asana update needed.")
            return
        elif errors["mismatch"]:
            for hubmap_id, value in errors["mismatch"]:
                response = client.tasks.update_task(
                    value["task_id"],
                    {
                        "custom_fields": {asana_hubmap_id_gid: value["status"]},
                    },
                )
                try:
                    response.raise_for_status()
                except requests.HTTPError as e:
                    logging.info(f"ERROR: Asana status not updated for {hubmap_id}: {e}")
                logging.info(f"Asana status updated for {hubmap_id}: {response['custom_fields']}")
        if errors["missing_key"]:
            logging.info(
                f"The following HuBMAP IDs were not found in Entity API: {errors['missing_key']}."
            )
        # This really should not happen but should be logged if it does
        if errors["missing_status"]:
            logging.info(
                f"The following HuBMAP IDs exist but did not have a status assigned in Entity API: {errors['missing_status']}."
            )

    t_update_asana_if_needed = PythonOperator(
        task_id="update_asana_if_needed",
        python_callable=update_asana_if_needed,
        provide_context=True,
    )

    t_get_asana_tasks >> t_query_entity_api >> t_compare_results >> t_update_asana_if_needed
