from datetime import datetime, timedelta
from pprint import pprint

from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
)
from utils import (
    HMDAG,
    get_dataset_uuid,
    get_parent_dataset_uuid,
    get_parent_dataset_uuids_list,
    get_preserve_scratch_resource,
    get_previous_revision_uuid,
    get_queue_resource,
    get_tmp_dir_path,
    pythonop_send_create_dataset,
    pythonop_set_dataset_state,
)

from airflow.operators.python import PythonOperator

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
    "queue": get_queue_resource("test_workflow"),
}

# trigger with
# {"parent_submission_id": <id>}

with HMDAG(
    "test_create_derived",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("test_workflow"),
    },
) as dag:

    pipeline_name = "test_create_derived"

    # should set on primary (new behavior)
    t_set_dataset_error_primary = PythonOperator(
        task_id="set_dataset_error",
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
            "parent_dataset_uuid_callable": get_parent_dataset_uuid,
            "pipeline_name": pipeline_name,
        },
    )

    t_send_create_dataset = PythonOperator(
        task_id="send_create_dataset",
        python_callable=pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs={
            "parent_dataset_uuid_callable": get_parent_dataset_uuids_list,
            "previous_revision_uuid_callable": get_previous_revision_uuid,
            "http_conn_id": "ingest_api_connection",
            "dataset_name_callable": lambda **kwargs: "test_derived_dataset",
            "pipeline_shorthand": pipeline_name,
        },
    )

    # should set on derived (existing unmodified behavior)
    t_set_dataset_error_derived = PythonOperator(
        task_id="set_dataset_error",
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
            "parent_dataset_uuid_callable": get_parent_dataset_uuid,
            "pipeline_name": pipeline_name,
        },
    )

    t_set_dataset_error_primary >> t_send_create_dataset >> t_set_dataset_error_derived
