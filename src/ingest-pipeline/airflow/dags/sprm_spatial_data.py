from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
    JoinOperator,
    LogInfoOperator,
    MoveDataOperator,
    SetDatasetProcessingOperator,
)

import utils
from utils import (
    get_absolute_workflow,
    get_dataset_uuid,
    get_parent_dataset_uuids_list,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    pythonop_get_dataset_state,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_dataset_type_previous_version,
    get_dataname_previous_version,
    build_provenance_function,
    get_assay_previous_version,
    get_cwl_cmd_from_workflows,
    gather_calculated_metadata,
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
    "queue": get_queue_resource("sprm_spatial_data"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}

with HMDAG(
    "sprm_spatial_data",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("sprm_spatial_data"),
    },
) as dag:
    pipeline_name = "sprm_spatial_data"

    cwl_workflows = [
        {
            "workflow_path": str(
                get_absolute_workflow(Path("sprm/steps", "create-spatial-data.cwl"))
            ),
            "documentation_url": "",
        }
    ]

    prepare_cwl1 = EmptyOperator(task_id="prepare_cwl1")

    def build_cwltool_cmd1(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # --image_dir /path/to/derived/dataset/pipeline_output/expr/
        # --mask_dir /path/to/derived/dataset/pipeline_output/mask/
        # --sprm_dir /path/to/derived/dataset/sprm_outputs/
        # --spatialdata_dir /path/to/derived/dataset/for_spatialdata/ should be appended to the end if that path exists

        input_parameters = [
            {
                "parameter_name": "--image_dir",
                "value": str(tmpdir / "pipeline_output/expr"),
            },
            {
                "parameter_name": "--mask_dir",
                "value": str(tmpdir / "pipeline_output/mask"),
            },
            {
                "parameter_name": "--sprm_dir",
                "value": str(tmpdir / "sprm_outputs"),
            }
        ]

        if os.path.exists(tmpdir / "for_spatialdata"):
            input_parameters.append(
                {
                    "parameter_name": "--spatial_data_dir",
                    "value": str(tmpdir / "for_spatialdata"),
                }
            )

        command = get_cwl_cmd_from_workflows(cwl_workflows, 0, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd1 = PythonOperator(
        task_id="build_cmd1",
        python_callable=build_cwltool_cmd1,
        provide_context=True,
    )

    t_populate_tmpdir = MoveDataOperator(task_id="populate_tmpdir")

    t_pipeline_exec_create_spatial_data = BashOperator(
        task_id="pipeline_exec_create_spatial_data",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl1 = BranchPythonOperator(
        task_id="maybe_keep_cwl1",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_data",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_create_spatial_data",
        },
    )

    # TODO: This likely needs to be fixed. Should probably just use all of the same info as the previous dataset.
    t_send_create_dataset = PythonOperator(
        task_id="send_create_dataset",
        python_callable=utils.pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs={
            "parent_dataset_uuid_callable": get_parent_dataset_uuids_list,
            "previous_revision_uuid_callable": get_previous_revision_uuid,
            "http_conn_id": "ingest_api_connection",
            "dataset_name_callable": get_dataname_previous_version,
            "dataset_type_callable": get_dataset_type_previous_version,
        },
    )

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
            "pipeline_name": pipeline_name
        },
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec_create_spatial_data",
            "move_data",
        ],
        cwl_workflows=cwl_workflows,
        no_provenance=True,
        metadata_fun=gather_calculated_metadata,
    )

    # TODO: This likely needs to be adjusted as well.
    build_provenance = build_provenance_function(
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd1"
        ),
    )

    t_build_provenance = PythonOperator(
        task_id="build_provenance",
        python_callable=build_provenance,
        provide_context=True,
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg",
        python_callable=send_status_msg,
        provide_context=True,
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_join = JoinOperator(task_id="join")
    t_move_data = MoveDataOperator(task_id="move_data")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir", trigger_rule="all_done")
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id="set_dataset_processing")

    (
        t_log_info
        >> t_create_tmpdir
        >> t_send_create_dataset
        >> t_set_dataset_processing
        >> t_populate_tmpdir

        >> prepare_cwl1
        >> t_build_cmd1
        >> t_pipeline_exec_create_spatial_data
        >> t_maybe_keep_cwl1

        >> t_move_data
        >> t_build_provenance
        >> t_send_status
        >> t_join
    )
    t_maybe_keep_cwl1 >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
