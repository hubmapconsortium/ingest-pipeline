from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

import utils
from utils import (
    pythonop_set_dataset_state,
    get_cwltool_base_cmd,
    get_parent_dataset_uuid,
    get_named_absolute_workflows,
    get_parent_dataset_uuids_list,
    get_parent_data_dir,
    build_dataset_name as inner_build_dataset_name,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    pythonop_get_dataset_state,
    get_dataset_type_organ_based,
)
from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
    JoinOperator,
    LogInfoOperator,
    MoveDataOperator,
    SetDatasetProcessingOperator,
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
    "queue": get_queue_resource("tsv_to_mudata"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}


with HMDAG(
    "tsv_to_mudata",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("pas_ftu_segmentation"),
    },
) as dag:
    pipeline_name = "epic-obj-csv-to-mudata"
    cwl_workflows = get_named_absolute_workflows(
        mudata=Path(pipeline_name, "pipeline.cwl"),
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    t_set_dataset_processing = PythonOperator(
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        op_kwargs={
            "dataset_uuid_callable": get_parent_dataset_uuid,
        },
        task_id="set_dataset_processing",
    )

    prepare_cwl_tsv_to_mudata = DummyOperator(task_id="prepare_cwl_tsv_to_mudata")

    # TODO: Add step here that converts the expected CSVs to TSVs

    def build_cwltool_cwl_tsv_to_mudata(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        workflow = cwl_workflows["mudata"]

        command = [
            *get_cwltool_base_cmd(tmpdir),
            # "--singularity",
            workflow,
            "--data_dir",
            data_dir,
        ]

        return join_quote_command_str(command)

    t_build_cwl_tsv_to_mudata = PythonOperator(
        task_id="build_cwl_tsv_to_mudata",
        python_callable=build_cwltool_cwl_tsv_to_mudata,
        provide_context=True,
    )

    t_pipeline_exec_cwl_tsv_to_mudata = BashOperator(
        task_id="pipeline_exec_cwl_tsv_to_mudata",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_tsv_to_mudata')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_tsv_to_mudata = BranchPythonOperator(
        task_id="maybe_keep_cwl_tsv_to_mudata",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "send_status_msg",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_tsv_to_mudata",
        },
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec_cwl_tsv_to_mudata",
            "move_data",
        ],
        cwl_workflows=list(cwl_workflows.values()),
        dataset_uuid_fun=get_parent_dataset_uuid,
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg", python_callable=send_status_msg, provide_context=True
    )

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": get_parent_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
        },
    )

    t_join = JoinOperator(task_id="join")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")

    (
        t_log_info
        >> t_create_tmpdir
        >> t_set_dataset_processing
        >> prepare_cwl_tsv_to_mudata
        >> t_build_cwl_tsv_to_mudata
        >> t_pipeline_exec_cwl_tsv_to_mudata
        >> t_maybe_keep_cwl_tsv_to_mudata
        >> t_send_status
        >> t_join
    )

    t_maybe_keep_cwl_tsv_to_mudata >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
