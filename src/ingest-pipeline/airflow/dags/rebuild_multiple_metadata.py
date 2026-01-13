from pprint import pprint

from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.configuration import conf as airflow_conf
from datetime import datetime
from airflow import DAG

from utils import (
    get_queue_resource,
    get_preserve_scratch_resource,
    get_soft_data,
    get_tmp_dir_path,
    encrypt_tok,
    pythonop_get_dataset_state,
    pythonop_build_dataset_lists
)


def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    return None


default_args = {
    "start_date": datetime(2019, 1, 1),
}

with DAG(
    "rebuild_multiple_metadata",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("rebuild_metadata"),
    },
) as dag:

    t_build_dataset_lists = PythonOperator(
        task_id="build_dataset_lists",
        python_callable=pythonop_build_dataset_lists,
        provide_context=True,
        queue=get_queue_resource("rebuild_metadata"),
        op_kwargs={
            "crypt_auth_tok": encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    def get_primary_dataset_uuids(**kwargs):
        return [{"uuid": uuid} for uuid in kwargs["dag_run"].conf["primary_datasets"]]

    t_get_primary_dataset_uuids = PythonOperator(
        task_id="get_primary_dataset_uuids",
        python_callable=get_primary_dataset_uuids,
        queue=get_queue_resource("rebuild_metadata"),
        provide_context=True,
    )

    def get_processed_dataset_uuids(**kwargs):
        return [{"uuid": uuid} for uuid in kwargs["dag_run"].conf["processed_datasets"]]

    t_get_processed_dataset_uuids = PythonOperator(
        task_id="get_processed_dataset_uuids",
        python_callable=get_processed_dataset_uuids,
        queue=get_queue_resource("rebuild_metadata"),
        provide_context=True,
    )

    def get_component_dataset_uuids(**kwargs):
        return [{"uuid": uuid} for uuid in kwargs["dag_run"].conf["component_datasets"]]

    t_get_component_dataset_uuids = PythonOperator(
        task_id="get_component_dataset_uuids",
        python_callable=get_component_dataset_uuids,
        queue=get_queue_resource("rebuild_metadata"),
        provide_context=True,
    )

    t_launch_rebuild_primary_dataset_metadata = TriggerDagRunOperator.partial(
        task_id="trigger_rebuild_primary_dataset_metadata",
        trigger_dag_id="rebuild_primary_dataset_metadata",
        queue=get_queue_resource("rebuild_metadata"),
    ).expand(conf=t_get_primary_dataset_uuids.output)

    t_launch_rebuild_processed_dataset_metadata = TriggerDagRunOperator.partial(
        task_id="trigger_rebuild_processed_dataset_metadata",
        trigger_dag_id="rebuild_processed_dataset_metadata",
        queue=get_queue_resource("rebuild_metadata"),
    ).expand(conf=t_get_processed_dataset_uuids.output)

    t_launch_rebuild_component_dataset_metadata = TriggerDagRunOperator.partial(
        task_id="trigger_rebuild_component_dataset_metadata",
        trigger_dag_id="multiassay_component_metadata",
        queue=get_queue_resource("rebuild_metadata"),
    ).expand(conf=t_get_component_dataset_uuids.output)

    t_build_dataset_lists >> [
        t_get_primary_dataset_uuids,
        t_get_processed_dataset_uuids,
        t_get_component_dataset_uuids,
    ]
    t_get_primary_dataset_uuids >> t_launch_rebuild_primary_dataset_metadata
    t_get_processed_dataset_uuids >> t_launch_rebuild_processed_dataset_metadata
    t_get_component_dataset_uuids >> t_launch_rebuild_component_dataset_metadata
