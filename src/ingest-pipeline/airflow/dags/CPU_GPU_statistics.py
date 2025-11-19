from airflow.decorators import task
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.configuration import conf as airflow_conf

from utils import (
    get_queue_resource,
    create_dataset_state_error_callback,
    get_uuid_for_error,
    HMDAG,
    encrypt_tok,
    get_tmp_dir_path,
    get_preserve_scratch_resource,
    get_auth_tok,
)

default_args = {
    "owner": "hubmap",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["joel.welling@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "xcom_push": True,
    "queue": get_queue_resource("codex_cytokit"),
    "on_failure_callback": create_dataset_state_error_callback(get_uuid_for_error),
}

with HMDAG(
    "CPU_GPU_statistics",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
    },
) as dag:

    def get_uuids(**kwargs):
        query = {
            "_source": [
                "entity_type", "creation_action", "dataset_type", "status", "uuid"
            ],
            "size": 10000,  # Adjust if you have more than 10k datasets
            "query": {
                "bool": {
                    "should": [
                        {"match": {"creation_action": "Central Process"}},  # Processed datasets
                    ],
                    "must": [
                        {"match": {"entity_type": "Dataset"}},
                        {"match": {"status": "Published"}},
                        {"match": {"status": "QA"}},
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        http_hook = HttpHook("POST", http_conn_id="search_api_connection")
        # TODO: find why the connection id isn't getting the v3 portion
        endpoint = f"/v3/portal/search"
        headers = {
            "Authorization": f"Bearer "
                             f"{get_auth_tok(kwargs['crypt_auth_tok'])}",
            "Content-Type": "application/json",
        }
        response = http_hook.run(endpoint=endpoint, headers=headers, data=query)
        response.raise_for_status()
        data = response.json()
        print(f"Long return {data}")
        uuid_list = [uuid for uuid in data]
        kwargs["ti"].xcom_push(key="uuid_list", value=uuid_list)
        return 0

    get_uuids_t = PythonOperator(
        task_id="get_uuids",
        python_callable=get_uuids,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )
    @task(task_id="calculate_statistics")
    def calculate_statistics(**kwargs):
        for uuid in kwargs["ti"].xcom_pull(task_id="get_uuids", key="uuid_list"):
            """Get path for uuid"""
            return uuid

    calculate_statistics_t = calculate_statistics()

    (
        get_uuids_t
        >> calculate_statistics_t
    )
