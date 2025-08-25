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
    get_threads_resource,
    get_parent_data_dir,
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
    "queue": get_queue_resource("azimuth_annotations"),
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
    workflow_version = "1.0.0"
    workflow_description = "SPM Spatial Data transformation"

    cwl_workflows = [
        {
            "workflow_path": str(
                get_absolute_workflow(Path("sprm-spatial-data", "pipeline.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("create-vis-symlink-archive", "pipeline.cwl"))
            ),
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
                get_absolute_workflow(Path("portal-containers", "sprm-to-json.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "sprm-to-anndata.cwl"))
            ),
            "documentation_url": "",
        },
    ]

    t_populate_tmpdir = MoveDataOperator(task_id="populate_tmpdir")

    prepare_cwl_sprm = EmptyOperator(task_id="prepare_cwl_sprm")

    def build_cwltool_cmd_sprm(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        input_parameters = [
            {"parameter_name": "--enable_manhole", "value": ""},
            {"parameter_name": "--processes", "value": get_threads_resource(dag.dag_id)},
            {"parameter_name": "--image_dir", "value": str(data_dir / "pipeline_output/expr")},
            {"parameter_name": "--mask_dir", "value": str(data_dir / "pipeline_output/mask")},
            {"parameter_name": "--cell_types_directory", "value": str(data_dir / "ribca_for_sprm")},
            {"parameter_name": "--cell_types_directory", "value": str(data_dir / "deepcelltypes")},
            {"parameter_name": "--cell_types_directory", "value": str(data_dir / "stellar")},
        ]

        command = get_cwl_cmd_from_workflows(cwl_workflows, 0, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)


    t_build_cmd_sprm = PythonOperator(
        task_id="build_cmd_sprm",
        python_callable=build_cwltool_cmd_sprm,
        provide_context=True,
    )

    t_pipeline_exec_cwl_sprm = BashOperator(
        task_id="pipeline_exec_cwl_sprm",
        bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cmd_sprm')}} >> ${tmp_dir}/session.log 2>&1 ; \
            echo $?
            """,
    )

    t_maybe_keep_cwl_sprm = BranchPythonOperator(
        task_id="maybe_keep_cwl_sprm",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_create_vis_symlink_archive",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_sprm",
        },
    )

    prepare_cwl_create_vis_symlink_archive = EmptyOperator(
        task_id="prepare_cwl_create_vis_symlink_archive",
    )

    def build_cwltool_cmd_create_vis_symlink_archive(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd_sprm")

        input_parameters = [
            {"parameter_name": "--ometiff_dir", "value": str(data_dir / "pipeline_output")},
            {"parameter_name": "--sprm_output", "value": str(data_dir / "sprm_outputs")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 1, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)


    t_build_cmd_create_vis_symlink_archive = PythonOperator(
        task_id="build_cmd_create_vis_symlink_archive",
        python_callable=build_cwltool_cmd_create_vis_symlink_archive,
        provide_context=True,
    )

    t_pipeline_exec_cwl_create_vis_symlink_archive = BashOperator(
        task_id="pipeline_exec_cwl_create_vis_symlink_archive",
        bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cmd_create_vis_symlink_archive')}} >> ${tmp_dir}/session.log 2>&1 ; \
            echo $?
            """,
    )

    t_maybe_keep_cwl_create_vis_symlink_archive = BranchPythonOperator(
        task_id="maybe_keep_cwl_create_vis_symlink_archive",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ome_tiff_pyramid",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_create_vis_symlink_archive",
        },
    )

    prepare_cwl_ome_tiff_pyramid = EmptyOperator(task_id="prepare_cwl_ome_tiff_pyramid")


    def build_cwltool_cwl_ome_tiff_pyramid(**kwargs):
        run_id = kwargs["run_id"]

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # data directory is the stitched images, which are found in tmpdir
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_create_vis_symlink_archive"
        )

        input_parameters = [
            {"parameter_name": "--processes", "value": get_threads_resource(dag.dag_id)},
            {"parameter_name": "--ometiff_directory", "value": str(tmpdir / "cwl_out")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 2, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)


    t_build_cmd_ome_tiff_pyramid = PythonOperator(
        task_id="build_cwl_ome_tiff_pyramid",
        python_callable=build_cwltool_cwl_ome_tiff_pyramid,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_pyramid = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_pyramid",
        bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cwl_ome_tiff_pyramid')}} >> $tmp_dir/session.log 2>&1 ; \
            echo $?
            """,
    )

    t_maybe_keep_cwl_ome_tiff_pyramid = BranchPythonOperator(
        task_id="maybe_keep_cwl_ome_tiff_pyramid",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_ome_tiff_offsets",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_pyramid",
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
            key="cwl_workflows", task_ids="build_cwl_ome_tiff_pyramid"
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
            "next_op": "prepare_cwl_sprm_to_json",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_offsets",
        },
    )

    prepare_cwl_sprm_to_json = EmptyOperator(task_id="prepare_cwl_sprm_to_json")


    def build_cwltool_cmd_sprm_to_json(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"  # This stage reads input from stage 1
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_ome_tiff_offsets"
        )

        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(data_dir / "sprm_outputs")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 4, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)


    t_build_cmd_sprm_to_json = PythonOperator(
        task_id="build_cmd_sprm_to_json",
        python_callable=build_cwltool_cmd_sprm_to_json,
        provide_context=True,
    )

    t_pipeline_exec_cwl_sprm_to_json = BashOperator(
        task_id="pipeline_exec_cwl_sprm_to_json",
        bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cmd_sprm_to_json')}} >> ${tmp_dir}/session.log 2>&1 ; \
            echo $?
            """,
    )

    t_maybe_keep_cwl_sprm_to_json = BranchPythonOperator(
        task_id="maybe_keep_cwl_sprm_to_json",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl_sprm_to_anndata",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_sprm_to_json",
        },
    )

    prepare_cwl_sprm_to_anndata = EmptyOperator(task_id="prepare_cwl_sprm_to_anndata")


    def build_cwltool_cmd_sprm_to_anndata(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"  # This stage reads input from stage 1
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd_sprm_to_json")

        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(data_dir / "sprm_outputs")},
        ]

        command = get_cwl_cmd_from_workflows(workflows, 5, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)


    t_build_cmd_sprm_to_anndata = PythonOperator(
        task_id="build_cmd_sprm_to_anndata",
        python_callable=build_cwltool_cmd_sprm_to_anndata,
        provide_context=True,
    )

    t_pipeline_exec_cwl_sprm_to_anndata = BashOperator(
        task_id="pipeline_exec_cwl_sprm_to_anndata",
        bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cmd_sprm_to_anndata')}} >> ${tmp_dir}/session.log 2>&1 ; \
            echo $?
            """,
    )

    t_maybe_keep_cwl_sprm_to_anndata = BranchPythonOperator(
        task_id="maybe_keep_cwl_sprm_to_anndata",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "maybe_create_dataset",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_sprm_to_anndata",
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
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
        },
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec_cwl_sprm",
            "pipeline_exec_cwl_create_vis_symlink_archive",
            "pipeline_exec_cwl_ome_tiff_offsets",
            "pipeline_exec_cwl_sprm_to_json",
            "pipeline_exec_cwl_sprm_to_anndata",
            "move_data",
        ],
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd_sprm_to_anndata"
        ),
        workflow_description=workflow_description,
        workflow_version=workflow_version,
    )

    build_provenance = build_provenance_function(
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd3"
        ),
    )

    t_build_provenance = PythonOperator(
        task_id="build_provenance_salmon",
        python_callable=build_provenance,
        provide_context=True,
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg_salmon",
        python_callable=send_status_msg,
        provide_context=True,
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_move_data = MoveDataOperator(task_id="move_data")
    t_join = JoinOperator(task_id="join_salmon", trigger_rule="one_success")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir", trigger_rule="all_done")
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id="set_dataset_processing")

    (
        t_log_info
        >> t_create_tmpdir
        >> t_send_create_dataset
        >> t_set_dataset_processing
        >> t_populate_tmpdir

        >> prepare_cwl_sprm
        >> t_build_cmd_sprm
        >> t_pipeline_exec_cwl_sprm
        >> t_maybe_keep_cwl_sprm

        >> prepare_cwl_create_vis_symlink_archive
        >> t_build_cmd_create_vis_symlink_archive
        >> t_pipeline_exec_cwl_create_vis_symlink_archive
        >> t_maybe_keep_cwl_create_vis_symlink_archive

        >> prepare_cwl_ome_tiff_pyramid
        >> t_build_cmd_ome_tiff_pyramid
        >> t_pipeline_exec_cwl_ome_tiff_pyramid
        >> t_maybe_keep_cwl_ome_tiff_pyramid

        >> prepare_cwl_ome_tiff_offsets
        >> t_build_cmd_ome_tiff_offsets
        >> t_pipeline_exec_cwl_ome_tiff_offsets
        >> t_maybe_keep_cwl_ome_tiff_offsets

        >> prepare_cwl_sprm_to_json
        >> t_build_cmd_sprm_to_json
        >> t_pipeline_exec_cwl_sprm_to_json
        >> t_maybe_keep_cwl_sprm_to_json

        >> prepare_cwl_sprm_to_anndata
        >> t_build_cmd_sprm_to_anndata
        >> t_pipeline_exec_cwl_sprm_to_anndata
        >> t_maybe_keep_cwl_sprm_to_anndata

        >> t_move_data
        >> t_build_provenance
        >> t_send_status
        >> t_join
    )

    t_maybe_keep_cwl_sprm >> t_set_dataset_error
    t_maybe_keep_cwl_create_vis_symlink_archive >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_pyramid >> t_set_dataset_error
    t_maybe_keep_cwl_ome_tiff_offsets >> t_set_dataset_error
    t_maybe_keep_cwl_sprm_to_json >> t_set_dataset_error
    t_maybe_keep_cwl_sprm_to_anndata >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
