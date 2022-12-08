from pprint import pprint
from datetime import datetime, timedelta
import yaml
from os.path import dirname, join
from pathlib import Path
import logging

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

import utils
from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    HMDAG,
    get_queue_resource,
)

# import export_and_backup.export_and_backup_plugin as export_and_backup_plugin
import export_and_backup.export_and_backup_plugin as export_and_backup_plugin

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

        ds_rslt = utils.pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        print("ds_rslt:")
        pprint(ds_rslt)

        for key in ["data_types"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        # TODO: do we want to restrict entity_type?
        if not ds_rslt["entity_type"] in ["Dataset"]:
            raise AirflowException(f"Entity {uuid} is not a Dataset")

        print(f"Finished uuid {ds_rslt['uuid']}")
        return ds_rslt  # causing it to be put into xcom

    t_find_uuid = PythonOperator(
        task_id="find_uuid",
        python_callable=find_uuid,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                utils.encrypt_tok(
                    airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
                ).decode()
            ),
        },
    )

    def find_plugins(**kwargs):
        info_dict = kwargs["ti"].xcom_pull(task_ids="find_uuid").copy()
        map_path = join(dirname(__file__), "export_and_backup_map.yml")
        with open(map_path, "r") as f:
            map = yaml.safe_load(f)
        plugins = []
        for record in map["export_and_backup_map"]:
            # TODO: not sure if we want to base plugin_map on data_types?
            if record["type"] in info_dict["data_types"]:
                plugins.extend(record["plugins"])
        info_dict["plugins"] = plugins
        return info_dict

    t_find_plugins = PythonOperator(
        task_id="find_plugins",
        python_callable=find_plugins,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                utils.encrypt_tok(
                    airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
                ).decode()
            ),
        },
    )

    def run_plugins(**kwargs):
        info_dict = kwargs["ti"].xcom_pull(task_ids="find_plugins").copy()
        for key in info_dict:
            logging.info(f"{key.upper()}: {info_dict[key]}")
        plugin_path = Path(export_and_backup_plugin.__file__).parent / "plugins"
        print("Plugin path: ", plugin_path)
        plugin_list = []
        for plugin in export_and_backup_plugin.export_and_backup_result_iter(
            plugin_path, **info_dict
        ):
            result = plugin.run_plugin()
            print(result)
        return info_dict

    #         diagnostic_result = plugin.diagnose()
    #         if diagnostic_result.problem_found():
    #             logging.info(f'Plugin "{plugin.description}" found problems:')
    #             for err_str in diagnostic_result.to_strings():
    #                 logging.info("    " + err_str)
    #         else:
    #             logging.info(f'Plugin "{plugin.description}" found no problem')
    #     return info_dict  # causing it to be put into xcom

    t_run_plugins = PythonOperator(
        task_id="run_plugins",
        python_callable=run_plugins,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                utils.encrypt_tok(
                    airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
                ).decode()
            ),
        },
    )

    t_find_uuid >> t_find_plugins >> t_run_plugins
