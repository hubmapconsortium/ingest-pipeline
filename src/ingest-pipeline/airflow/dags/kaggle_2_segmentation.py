from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

import utils
from utils import (
    get_dataset_uuid,
    get_absolute_workflow,
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
    get_threads_resource,
    get_cwl_cmd_from_workflows,
)
from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
    JoinOperator,
    LogInfoOperator,
    MoveDataOperator,
    SetDatasetProcessingOperator,
)

from extra_utils import build_tag_containers

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
    "queue": get_queue_resource("kaggle_2_segmentation"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}


with HMDAG(
    "kaggle_2_segmentation",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("kaggle_2_segmentation"),
    },
) as dag:
    pipeline_name = "kaggle-2-segmentation"
    workflow_version = "1.0.0"
    workflow_description = "The Kaggle 2 pipeline segments crypts of Lieberkühn in H&E-stained histology images of intestine."
    cwl_workflows = [
        {
            "workflow_path": str(get_absolute_workflow(Path(pipeline_name, "pipeline.cwl"))),
            "documentation_url": "",
        },
        {
            "workflow_path": str(get_absolute_workflow(Path("ome-tiff-pyramid", "pipeline.cwl"))),
            "documentation_url": "",
        },
        {
            "workflow_path": str(get_absolute_workflow(Path("ome-tiff-pyramid", "pipeline.cwl"))),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "ome-tiff-offsets.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "ome-tiff-metadata.cwl"))
            ),
            "documentation_url": "",
        },
    ]

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    @task(task_id="prepare_cwl_segmentation")
    def prepare_cwl_cmd1(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[0]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_segmentation = prepare_cwl_cmd1()

    def build_cwltool_cwl_segmentation(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        # get organ type
        ds_rslt = pythonop_get_dataset_state(
            dataset_uuid_callable=lambda **kwargs:
            get_parent_dataset_uuids_list(**kwargs)[0], **kwargs)

        organ_list = list(set(ds_rslt["organs"]))
        organ_code = organ_list[0] if len(organ_list) == 1 else "multi"

        input_parameters = [
            {"parameter_name": "--data_directory", "value": str(data_dir)},
            {"parameter_name": "--tissue_type", "value": organ_code},
        ]
        command = get_cwl_cmd_from_workflows(
            cwl_workflows,
            0,
            input_parameters,
            tmpdir,
            kwargs["ti"],
        )

        return join_quote_command_str(command)

    t_build_cwl_segmentation = PythonOperator(
        task_id="build_cwl_segmentation",
        python_callable=build_cwltool_cwl_segmentation,
        provide_context=True,
    )

    t_pipeline_exec_cwl_segmentation = BashOperator(
        task_id="pipeline_exec_cwl_segmentation",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_segmentation')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_segmentation = BranchPythonOperator(
        task_id="maybe_keep_cwl_segmentation",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ome_tiff_pyramid",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_segmentation",
        },
    )

    prepare_cwl_ome_tiff_pyramid = EmptyOperator(task_id="prepare_cwl_ome_tiff_pyramid")

    def build_cwltool_cwl_ome_tiff_pyramid_processed(**kwargs):
        run_id = kwargs["run_id"]

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cwl_segmentation")

        input_parameters = [
            {"parameter_name": "--processes", "value": get_threads_resource(dag.dag_id)},
            {"parameter_name": "--ometiff_directory", "value": str(tmpdir / "cwl_out")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 1, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_ome_tiff_pyramid_processed = PythonOperator(
        task_id="build_cwl_ome_tiff_pyramid_processed",
        python_callable=build_cwltool_cwl_ome_tiff_pyramid_processed,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_pyramid_processed = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_pyramid_processed",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cwl_ome_tiff_pyramid_processed')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_ome_tiff_pyramid_processed = BranchPythonOperator(
        task_id="maybe_keep_cwl_ome_tiff_pyramid_processed",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "build_cwl_ome_tiff_pyramid_raw",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_pyramid_processed",
        },
    )

    def build_cwltool_cwl_ome_tiff_pyramid_raw(**kwargs):
        run_id = kwargs["run_id"]

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # data directory is the primary dataset's directory
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cwl_ome_tiff_pyramid_processed"
        )

        input_parameters = [
            {"parameter_name": "--processes", "value": get_threads_resource(dag.dag_id)},
            {"parameter_name": "--ometiff_directory", "value": str(data_dir)},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 2, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_ome_tiff_pyramid_raw = PythonOperator(
        task_id="build_cwl_ome_tiff_pyramid_raw",
        python_callable=build_cwltool_cwl_ome_tiff_pyramid_raw,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_pyramid_raw = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_pyramid_raw",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cwl_ome_tiff_pyramid_raw')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_ome_tiff_pyramid_raw = BranchPythonOperator(
        task_id="maybe_keep_cwl_ome_tiff_pyramid_raw",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ome_tiff_offsets",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_pyramid_raw",
        },
    )

    prepare_cwl_ome_tiff_offsets = EmptyOperator(task_id="prepare_cwl_ome_tiff_offsets")

    def build_cwltool_cmd_ome_tiff_offsets(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cwl_ome_tiff_pyramid_raw"
        )

        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(data_dir / "ometiff-pyramids")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 3, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_ome_tiff_offsets = PythonOperator(
        task_id="build_cmd_ome_tiff_offsets",
        python_callable=build_cwltool_cmd_ome_tiff_offsets,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_offsets = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_offsets",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_ome_tiff_offsets')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_ome_tiff_offsets = BranchPythonOperator(
        task_id="maybe_keep_cwl_ome_tiff_offsets",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ome_tiff_metadata",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_offsets",
        },
    )

    prepare_cwl_ome_tiff_metadata = EmptyOperator(task_id="prepare_cwl_ome_tiff_metadata")

    def build_cwltool_cmd_ome_tiff_metadata(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_ome_tiff_offsets"
        )

        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(data_dir / "ometiff-pyramids")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 4, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    t_build_cmd_ome_tiff_metadata = PythonOperator(
        task_id="build_cmd_ome_tiff_metadata",
        python_callable=build_cwltool_cmd_ome_tiff_metadata,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_metadata = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_metadata",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd_ome_tiff_metadata')}} >> ${tmp_dir}/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_ome_tiff_metadata = BranchPythonOperator(
        task_id="maybe_keep_cwl_ome_tiff_metadata",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "maybe_create_dataset",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_metadata",
        },
    )

    t_maybe_create_dataset = BranchPythonOperator(
        task_id="maybe_create_dataset",
        python_callable=utils.pythonop_dataset_dryrun,
        provide_context=True,
        op_kwargs={
            "next_op": "send_create_dataset",
            "bail_op": "join",
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
            "dataset_name_callable": build_dataset_name,
            "dataset_type_callable": get_dataset_type_organ_based,
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

    t_expand_symlinks = BashOperator(
        task_id="expand_symlinks",
        bash_command="""
        tmp_dir="{{tmp_dir_path(run_id)}}" ; \
        ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
        groupname="{{conf.as_dict()['connections']['OUTPUT_GROUP_NAME']}}" ; \
        cd "$ds_dir" ; \
        tar -xf symlinks.tar ; \
        echo $?
        """,
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec_cwl_segmentation",
            "pipeline_exec_cwl_ome_tiff_offsets",
            "move_data",
        ],
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_ome_tiff_metadata"
        ),
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg", python_callable=send_status_msg, provide_context=True
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_join = JoinOperator(task_id="join")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id="set_dataset_processing")
    t_move_data = MoveDataOperator(task_id="move_data")

    (
        t_log_info
        >> t_create_tmpdir

        >> prepare_cwl_segmentation
        >> t_build_cwl_segmentation
        >> t_pipeline_exec_cwl_segmentation
        >> t_maybe_keep_cwl_segmentation

        >> prepare_cwl_ome_tiff_pyramid
        >> t_build_cmd_ome_tiff_pyramid_processed
        >> t_pipeline_exec_cwl_ome_tiff_pyramid_processed
        >> t_maybe_keep_cwl_ome_tiff_pyramid_processed
        >> t_build_cmd_ome_tiff_pyramid_raw
        >> t_pipeline_exec_cwl_ome_tiff_pyramid_raw
        >> t_maybe_keep_cwl_ome_tiff_pyramid_raw

        >> prepare_cwl_ome_tiff_offsets
        >> t_build_cmd_ome_tiff_offsets
        >> t_pipeline_exec_cwl_ome_tiff_offsets
        >> t_maybe_keep_cwl_ome_tiff_offsets

        >> prepare_cwl_ome_tiff_metadata
        >> t_build_cmd_ome_tiff_metadata
        >> t_pipeline_exec_cwl_ome_tiff_metadata
        >> t_maybe_keep_cwl_ome_tiff_metadata
        >> t_maybe_create_dataset

        >> t_send_create_dataset
        >> t_move_data
        >> t_expand_symlinks
        >> t_send_status
        >> t_join
    )
    t_maybe_keep_cwl_segmentation >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_pyramid_raw >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_pyramid_processed >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_offsets >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
