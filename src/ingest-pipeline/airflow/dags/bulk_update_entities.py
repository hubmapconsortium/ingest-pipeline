from pprint import pprint
import time

from airflow.operators.python import PythonOperator
from airflow.configuration import conf as airflow_conf
from datetime import datetime
from airflow import DAG
from airflow.hooks.http_hook import HttpHook

from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    get_preserve_scratch_resource,
    get_tmp_dir_path,
    encrypt_tok,
    pythonop_get_dataset_state,
    get_auth_tok,
)

default_args = {
    "start_date": datetime(2019, 1, 1),
}

with DAG(
    "bulk_update_entities",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("rebuild_metadata"),
    },
) as dag:

    def check_uuids(**kwargs):
        print("dag_run conf follows:")
        pprint(kwargs["dag_run"].conf)

        try:
            assert_json_matches_schema(kwargs["dag_run"].conf, "launch_multi_metadata_schema.yml")
        except AssertionError as e:
            print("invalid DAG metadata follows:")
            pprint(kwargs["dag_run"].conf)
            raise

        uuids = kwargs["dag_run"].conf["uuids"]
        filtered_uuids = []
        for uuid in uuids:
            # If this fails out then we know its something other than an upload or dataset
            try:
                pythonop_get_dataset_state(dataset_uuid_callable=lambda **kwargs: uuid, **kwargs)
                filtered_uuids.append(uuid)
            except Exception as e:
                print(f"{uuid} is neither a dataset nor an upload and will be skipped.")
                print(repr(e))

        kwargs["ti"].xcom_push(key="uuids", value=filtered_uuids)

    check_uuids_t = PythonOperator(
        task_id="check_uuids",
        python_callable=check_uuids,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    def update_uuids(**kwargs):
        auth_tok = get_auth_tok(**kwargs)
        headers = {
            "content-type": "application/json",
            "X-Hubmap-Application": "ingest-pipeline",
            "Authorization": f"Bearer {auth_tok}",
        }

        http_hook = HttpHook("PUT", http_conn_id="entity_api_connection")
        uuids = kwargs["dag_run"].conf["uuids"]

        for uuid in uuids:
            endpoint = f"entities/{uuid}"
            response = http_hook.run(endpoint, headers=headers)
            print("response: ")
            pprint(response.json())
            time.sleep(10)

    update_uuids_t = PythonOperator(
        task_id="update_uuids",
        python_callable=update_uuids,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    check_uuids_t >> update_uuids_t
