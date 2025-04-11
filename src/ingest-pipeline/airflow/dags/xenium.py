from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.decorators import task

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
    get_parent_data_dir,
    build_dataset_name as inner_build_dataset_name,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    HMDAG,
    pythonop_get_dataset_state,
    get_queue_resource,
    get_threads_resource,
    get_preserve_scratch_resource,
    get_cwl_cmd_from_workflows,
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
    "queue": get_queue_resource("xenium"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}

with HMDAG(
    "xenium",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("xenium"),
    },
) as dag:
    workflow_version = "1.0.0"
    workflow_description = "The pipeline for Xenium data converts the vendor's segmentation and quantification results to the AnnData and SpatialData formats, which are used by ScanPy for downstream analysis including dimensionality reduction, unsupervised clustering, and differential expression analysis. These segmentation and quantification results are also used by SquidPy to perform some spatial analysis on the data."

    cwl_workflows = [
        {
            "workflow_path": str(get_absolute_workflow(Path("xenium-pipeline", "pipeline.cwl"))),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "h5ad-to-arrow.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "anndata-to-ui.cwl"))
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
    ]

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, "salmon-rnaseq", **kwargs)


    @task(task_id="prepare_cwl1")
    def prepare_cwl_cmd1(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[0]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"


    prepare_cwl1 = prepare_cwl_cmd1()

    prepare_cwl2 = DummyOperator(task_id="prepare_cwl2")

    prepare_cwl3 = DummyOperator(task_id="prepare_cwl3")

    prepare_cwl4 = DummyOperator(task_id="prepare_cwl4")

    prepare_cwl5 = DummyOperator(task_id="prepare_cwl5")

    def build_cwltool_cmd1(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        data_dir = get_parent_data_dir(**kwargs)
        print("data_dirs: ", data_dir)

        source_type = ""
        unique_source_types = set()
        for parent_uuid in get_parent_dataset_uuids_list(**kwargs):
            dataset_state = pythonop_get_dataset_state(
                dataset_uuid_callable=lambda **kwargs: parent_uuid, **kwargs
            )
            source_type = dataset_state.get("source_type")
            if source_type == "mixed":
                print("Force failure. Should only be one unique source_type for a dataset.")
            else:
                unique_source_types.add(source_type)

        if len(unique_source_types) > 1:
            print("Force failure. Should only be one unique source_type for a dataset.")
        else:
            source_type = unique_source_types.pop().lower()

        cwl_parameters = [
            {"parameter_name": "--parallel", "value": ""},
        ]
        input_parameters = [
            {"parameter_name": "--assay", "value": "xenium"},
            {"parameter_name": "--data_dir", "value": str(data_dir)},
        ]

        command = get_cwl_cmd_from_workflows(
            cwl_workflows, 0, input_parameters, tmpdir, kwargs["ti"], cwl_parameters
        )

        return join_quote_command_str(command)

    def build_cwltool_cmd2(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd1")

        cwl_parameters = [
            {"parameter_name": "--outdir", "value": str(tmpdir / "cwl_out/hubmap_ui")},
        ]
        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(tmpdir / "cwl_out")},
        ]
        command = get_cwl_cmd_from_workflows(
            workflows, 1, input_parameters, tmpdir, kwargs["ti"], cwl_parameters
        )

        return join_quote_command_str(command)

    def build_cwltool_cmd3(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd2")

        cwl_parameters = [
            {"parameter_name": "--outdir", "value": str(tmpdir / "cwl_out/hubmap_ui")},
        ]
        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(tmpdir / "cwl_out")},
        ]
        command = get_cwl_cmd_from_workflows(
            workflows, 2, input_parameters, tmpdir, kwargs["ti"], cwl_parameters
        )

        return join_quote_command_str(command)

    def build_cwltool_cmd4(**kwargs):
        run_id = kwargs["run_id"]

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # data directory is the stitched images, which are found in tmpdir
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd3")

        input_parameters = [
            {"parameter_name": "--processes", "value": get_threads_resource(dag.dag_id)},
            {
                "parameter_name": "--ometiff_directory",
                "value": str(data_dir / "lab_processed/images/"),
            },
        ]
        command = get_cwl_cmd_from_workflows(workflows, 3, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    def build_cwltool_cmd5(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd4")

        input_parameters = [
            {"parameter_name": "--input_dir", "value": str(data_dir / "ometiff-pyramids")},
        ]
        command = get_cwl_cmd_from_workflows(workflows, 4, input_parameters, tmpdir, kwargs["ti"])

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

    t_build_cmd3 = PythonOperator(
        task_id="build_cmd3",
        python_callable=build_cwltool_cmd3,
        provide_context=True,
    )

    t_build_cmd4 = PythonOperator(
        task_id="build_cmd4",
        python_callable=build_cwltool_cmd4,
        provide_context=True,
    )

    t_build_cmd5 = PythonOperator(
        task_id="build_cmd5",
        python_callable=build_cwltool_cmd5,
        provide_context=True,
    )

    t_pipeline_exec = BashOperator(
        task_id="pipeline_exec",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_convert_for_ui = BashOperator(
        task_id="convert_for_ui",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
        mkdir -p ${tmp_dir}/cwl_out/hubmap_ui ; \
        {{ti.xcom_pull(task_ids='build_cmd2')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_convert_for_ui_2 = BashOperator(
        task_id="convert_for_ui_2",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
        {{ti.xcom_pull(task_ids='build_cmd3')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_pipeline_exec_cwl_ome_tiff_pyramid = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_pyramid",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd4')}} >> $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_pipeline_exec_cwl_ome_tiff_offsets = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_offsets",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd5')}} >> ${tmp_dir}/session.log 2>&1 ; \
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
            "test_op": "pipeline_exec",
        },
    )

    t_maybe_keep_cwl2 = BranchPythonOperator(
        task_id="maybe_keep_cwl2",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl3",
            "bail_op": "set_dataset_error",
            "test_op": "convert_for_ui",
        },
    )

    t_maybe_keep_cwl3 = BranchPythonOperator(
        task_id="maybe_keep_cwl3",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl4",
            "bail_op": "set_dataset_error",
            "test_op": "convert_for_ui_2",
        },
    )

    t_maybe_keep_cwl4 = BranchPythonOperator(
        task_id="maybe_keep_cwl4",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl5",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_pyramid",
        },
    )

    t_maybe_keep_cwl5 = BranchPythonOperator(
        task_id="maybe_keep_cwl5",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_create_dataset",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_offsets",
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
            "pipeline_shorthand": "SpatialData + Scanpy",
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
            "message": f"An error occurred in xenium-pipeline",
        },
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=["pipeline_exec",
                     "move_data",
                     "convert_for_ui",
                     "convert_for_ui_2",
                     "pipeline_exec_cwl_ome_tiff_pyramid",
                     "pipeline_exec_cwl_ome_tiff_offsets"],
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd5"
        ),
        workflow_description=workflow_description,
        workflow_version=workflow_version,
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg",
        python_callable=send_status_msg,
        provide_context=True,
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_join = JoinOperator(task_id="join")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")
    t_move_data = MoveDataOperator(task_id="move_data")

    (
        t_log_info
        >> t_create_tmpdir

        >> prepare_cwl1
        >> t_build_cmd1
        >> t_pipeline_exec
        >> t_maybe_keep_cwl1

        >> prepare_cwl2
        >> t_build_cmd2
        >> t_convert_for_ui
        >> t_maybe_keep_cwl2

        >> prepare_cwl3
        >> t_build_cmd3
        >> t_convert_for_ui_2
        >> t_maybe_keep_cwl3

        >> prepare_cwl4
        >> t_build_cmd4
        >> t_pipeline_exec_cwl_ome_tiff_pyramid
        >> t_maybe_keep_cwl4

        >> prepare_cwl5
        >> t_build_cmd5
        >> t_pipeline_exec_cwl_ome_tiff_offsets
        >> t_maybe_keep_cwl5
        >> t_maybe_create_dataset

        >> t_send_create_dataset
        >> t_move_data
        >> t_send_status
        >> t_join
    )
    t_maybe_keep_cwl1 >> t_set_dataset_error
    t_maybe_keep_cwl2 >> t_set_dataset_error
    t_maybe_keep_cwl3 >> t_set_dataset_error
    t_maybe_keep_cwl4 >> t_set_dataset_error
    t_maybe_keep_cwl5 >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_maybe_create_dataset >> t_join
    t_join >> t_cleanup_tmpdir
