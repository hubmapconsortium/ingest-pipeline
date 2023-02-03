from datetime import datetime, timedelta
import logging
from os.path import dirname, join
from pathlib import Path
from pprint import pprint
import yaml

from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from utils import (
    HMDAG,
    encrypt_tok,
    get_queue_resource,
    localized_assert_json_matches_schema as assert_json_matches_schema,
    pythonop_get_dataset_state,
    encrypt_tok,
)

import export_and_backup.export_and_backup_plugin as export_and_backup_plugin

PLUGIN_MAP_FILENAME = "export_and_backup_map.yml"

# Following are defaults which can be overridden later on
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
    "queue": get_queue_resource("validation_test"),
}

with HMDAG(
    "export_and_backup",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
) as dag:

    # Pared down and altered copy of find_uuid from diagnose_failure
    def find_uuid(**kwargs):
        try:
            assert_json_matches_schema(kwargs["dag_run"].conf, "export_and_backup.yml")
        except AssertionError:
            print("invalid metadata follows:")
            pprint(kwargs["dag_run"].conf)
            raise

        uuid = kwargs["dag_run"].conf["uuid"]

        def my_callable(**kwargs):
            return uuid

        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        ds_rslt["crypt_auth_tok"] = kwargs["crypt_auth_tok"]
        print("ds_rslt:")
        pprint(ds_rslt)

        if not ds_rslt["entity_type"] in ["Dataset", "Upload"]:
            raise AirflowException(
                f"Entity {uuid} is entity_type {ds_rslt['entity_type']}, needs to be type 'Dataset' or 'Upload'"
            )

        if not ds_rslt["status"]:
            raise AirflowException(f"Entity {uuid} has no status")

        print(f'Finished uuid {ds_rslt["uuid"]}')
        return ds_rslt  # causing it to be put into xcom

    t_find_uuid = PythonOperator(
        task_id="find_uuid",
        python_callable=find_uuid,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            ),
        },
    )

    def find_plugins(**kwargs):
        info_dict = kwargs["ti"].xcom_pull(task_ids="find_uuid").copy()
        status = info_dict["status"].lower()
        entity_type = info_dict["entity_type"].lower()
        map_path = join(dirname(__file__), PLUGIN_MAP_FILENAME)
        with open(map_path, "r") as f:
            plugin_map = yaml.safe_load(f)
        plugins = []
        for entity, value in plugin_map["export_and_backup_map"].items():
            if not entity == entity_type:
                continue
            for status_type in value:
                if status_type["status"] == status:
                    plugins.extend(status_type["plugins"])
        info_dict["plugins"] = plugins
        return info_dict

    t_find_plugins = PythonOperator(
        task_id="find_plugins",
        python_callable=find_plugins,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            ),
        },
    )

    def run_plugins(**kwargs):
        info_dict = kwargs["ti"].xcom_pull(task_ids="find_plugins").copy()
        for key in info_dict:
            logging.info(f"{key.upper()}: {info_dict[key]}")
        plugin_path = Path(export_and_backup_plugin.__file__).parent / "plugins"
        print("plugin_path: ", plugin_path)
        for plugin in export_and_backup_plugin.export_and_backup_result_iter(
            plugin_path, **info_dict
        ):

            result = plugin.run_plugin()
            print("result: ", result)
        return info_dict

    t_run_plugins = PythonOperator(
        task_id="run_plugins",
        python_callable=run_plugins,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            ),
        },
    )

    t_find_uuid >> t_find_plugins >> t_run_plugins
