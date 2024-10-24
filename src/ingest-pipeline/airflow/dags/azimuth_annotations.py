from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
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
    get_absolute_workflows,
    get_cwltool_base_cmd,
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
    "queue": get_queue_resource("azimuth_annotations"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}

with HMDAG(
    "azimuth_annotations",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("azimuth_annotations"),
    },
) as dag:
    pipeline_name = "azimuth_annotate"
    cwl_workflows_files = get_absolute_workflows(
        Path("salmon-rnaseq", "pipeline.cwl"),
        Path("azimuth-annotate", "pipeline.cwl"),
        Path("portal-containers", "h5ad-to-arrow.cwl"),
        Path("portal-containers", "anndata-to-ui.cwl"),
    )
    cwl_workflows_annotations = get_absolute_workflows(
        Path("azimuth-annotate", "pipeline.cwl"),
        Path("portal-containers", "h5ad-to-arrow.cwl"),
        Path("portal-containers", "anndata-to-ui.cwl"),
        Path("portal-containers", "mudata-to-ui.cwl")
    )

    prepare_cwl1 = DummyOperator(task_id="prepare_cwl1")

    prepare_cwl2 = DummyOperator(task_id="prepare_cwl2")

    prepare_cwl3 = DummyOperator(task_id="prepare_cwl3")

    def build_cwltool_cmd1(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # get organ type
        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=get_dataset_uuid, **kwargs)

        organ_list = list(set(ds_rslt["organs"]))
        organ_code = organ_list[0] if len(organ_list) == 1 else "multi"
        assay, matrix, secondary_analysis, _ = get_assay_previous_version(**kwargs)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows_annotations[0],
            "--reference",
            organ_code,
            "--matrix",
            matrix,
            "--secondary-analysis-matrix",
            secondary_analysis,
            "--assay",
            assay,
        ]

        return join_quote_command_str(command)

    def build_cwltool_cmd2(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        assay, matrix, secondary_analysis, workflow = get_assay_previous_version(**kwargs)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows_annotations[workflow],
            "--input_dir",
            # This pipeline invocation runs in a 'hubmap_ui' subdirectory,
            # so use the parent directory as input
            "..",
        ]
        kwargs["ti"].xcom_push(key="skip_cwl3", value=0 if workflow == 1 else 1)

        return join_quote_command_str(command)

    def build_cwltool_cmd4(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows_annotations[2],
            "--input_dir",
            # This pipeline invocation runs in a 'hubmap_ui' subdirectory,
            # so use the parent directory as input
            "..",
        ]

        return join_quote_command_str(command)

    t_build_cmd1 = PythonOperator(
        task_id="build_cmd1",
        python_callable=build_cwltool_cmd1,
        provide_context=True,
    )

    t_build_cmd2 = PythonOperator(
        task_id="build_cmd2",
        python_callable=build_cwltool_cmd2,
        provide_context=True,
    )

    t_build_cmd4 = PythonOperator(
        task_id="build_cmd4",
        python_callable=build_cwltool_cmd4,
        provide_context=True,
    )

    t_populate_tmpdir = MoveDataOperator(task_id="populate_tmpdir")

    t_pipeline_exec_azimuth_annotate = BashOperator(
        task_id="pipeline_exec_azimuth_annotate",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        cd "$tmp_dir"/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_convert_for_ui = BashOperator(
        task_id="convert_for_ui",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
        cd "$tmp_dir"/cwl_out ; \
        mkdir -p hubmap_ui ; \
        cd hubmap_ui ; \
        {{ti.xcom_pull(task_ids='build_cmd2')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_convert_for_ui_2 = BashOperator(
        task_id="convert_for_ui_2",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
        cd "$tmp_dir"/cwl_out ; \
        mkdir -p hubmap_ui ; \
        cd hubmap_ui ; \
        {{ti.xcom_pull(task_ids='build_cmd4')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl1 = BranchPythonOperator(
        task_id="maybe_keep_cwl1",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl2",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_azimuth_annotate",
        },
    )

    t_maybe_keep_cwl2 = BranchPythonOperator(
        task_id="maybe_keep_cwl2",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "maybe_skipp_cwl3",
            "bail_op": "set_dataset_error",
            "test_op": "convert_for_ui",
        },
    )

    t_maybe_skip_cwl3 = BranchPythonOperator(
        task_id="maybe_skip_cwl3",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_data",
            "bail_op": "prepare_cwl3",
            "test_op": "build_cmd2",
            "test_key": "skip_cwl3",
        },
    )

    t_maybe_keep_cwl3 = BranchPythonOperator(
        task_id="maybe_keep_cwl3",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_data",
            "bail_op": "set_dataset_error",
            "test_op": "convert_for_ui_2",
        },
    )

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
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
        },
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec",
            "pipeline_exec_azimuth_annotate",
            "move_data",
            "convert_for_ui",
            "convert_for_ui_2",
        ],
        cwl_workflows=cwl_workflows_files,
        no_provenance=True,
    )

    build_provenance = build_provenance_function(
        cwl_workflows=cwl_workflows_annotations,
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
    t_move_data = MoveDataOperator(task_id="move_data")
    t_join = JoinOperator(task_id="join")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id="set_dataset_processing")

    (
        t_log_info
        >> t_create_tmpdir
        >> t_send_create_dataset
        >> t_set_dataset_processing
        >> t_populate_tmpdir
        >> prepare_cwl1
        >> t_build_cmd1
        >> t_pipeline_exec_azimuth_annotate
        >> t_maybe_keep_cwl1
        >> prepare_cwl2
        >> t_build_cmd2
        >> t_convert_for_ui
        >> t_maybe_keep_cwl2
        >> t_maybe_skip_cwl3
        >> prepare_cwl3
        >> t_build_cmd4
        >> t_convert_for_ui_2
        >> t_maybe_keep_cwl3
        >> t_move_data
        >> t_build_provenance
        >> t_send_status
        >> t_join
    )
    t_maybe_keep_cwl1 >> t_set_dataset_error
    t_maybe_keep_cwl2 >> t_set_dataset_error
    t_maybe_keep_cwl3 >> t_set_dataset_error
    t_move_data >> t_build_provenance >> t_send_status >> t_join
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
