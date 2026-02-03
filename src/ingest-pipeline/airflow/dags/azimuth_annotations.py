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

    cwl_workflows_annotations_salmon = [
        {
            "workflow_path": str(
                get_absolute_workflow(Path("pan-organ-azimuth-annotate", "pipeline.cwl"))
            ),
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
    ]
    cwl_workflows_annotations_multiome = [
        {
            "workflow_path": str(
                get_absolute_workflow(Path("pan-organ-azimuth-annotate", "pipeline.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "mudata-to-ui.cwl"))
            ),
            "documentation_url": "",
        },
    ]

    cwl_workflows_files_salmon = [
        {
            "workflow_path": str(get_absolute_workflow(Path("salmon-rnaseq", "pipeline.cwl"))),
            "input_parameters": [
                {"parameter_name": "--assay", "value": ""},
                {"parameter_name": "--threads", "value": ""},
                {"parameter_name": "--organism", "value": ""},
                {"parameter_name": "--fastq_dir", "value": []},
            ],
            "documentation_url": "",
        },
        *cwl_workflows_annotations_salmon,
    ]
    cwl_workflows_files_multiome = [
        {
            "workflow_path": str(
                get_absolute_workflow(Path("multiome-rna-atac-pipeline", "pipeline.cwl"))
            ),
            "input_parameters": [
                {"parameter_name": "--threads_rna", "value": ""},
                {"parameter_name": "--threads_atac", "value": ""},
                {"parameter_name": "--organism", "value": ""},
                {"parameter_name": "--assay_rna", "value": ""},
                {"parameter_name": "--fastq_dir_rna", "value": []},
                {"parameter_name": "--assay_atac", "value": ""},
                {"parameter_name": "--fastq_dir_atac", "value": []},
            ],
            "documentation_url": "",
        },
        *cwl_workflows_annotations_multiome,
    ]

    prepare_cwl1 = EmptyOperator(task_id="prepare_cwl1")

    prepare_cwl2 = EmptyOperator(task_id="prepare_cwl2")

    prepare_cwl3 = EmptyOperator(task_id="prepare_cwl3")

    def build_cwltool_cmd1(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # get organ type
        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=get_dataset_uuid, **kwargs)

        source_type = ds_rslt.get("source_type", "human")
        if source_type == "mixed":
            print("Force failure. Should only be one unique source_type for a dataset.")

        _, _, secondary_analysis, _ = get_assay_previous_version(**kwargs)

        input_parameters = [
            {
                "parameter_name": "--secondary_analysis_matrix",
                "value": str(tmpdir / "cwl_out" / secondary_analysis),
            },
            {"parameter_name": "--organism", "value": source_type},
        ]

        workflows = (
            cwl_workflows_annotations_multiome
            if "mudata" in secondary_analysis
            else cwl_workflows_annotations_salmon
        )

        command = get_cwl_cmd_from_workflows(workflows, 0, input_parameters, tmpdir, kwargs["ti"])

        return join_quote_command_str(command)

    def build_cwltool_cmd2(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        assay, matrix, secondary_analysis, workflow = get_assay_previous_version(**kwargs)

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
        kwargs["ti"].xcom_push(key="skip_cwl3", value=1 if workflow == 0 else 0)

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

    t_populate_tmpdir = MoveDataOperator(task_id="populate_tmpdir")

    t_pipeline_exec_azimuth_annotate = BashOperator(
        task_id="pipeline_exec_azimuth_annotate",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} >> $tmp_dir/session.log 2>&1 ; \
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
            "next_op": "maybe_skip_cwl3",
            "bail_op": "set_dataset_error",
            "test_op": "convert_for_ui",
        },
    )

    t_maybe_skip_cwl3 = BranchPythonOperator(
        task_id="maybe_skip_cwl3",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_data_multiome",
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
            "next_op": "move_data_salmon",
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
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
            "pipeline_name": pipeline_name
        },
    )

    send_status_msg_salmon = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec",
            "pipeline_exec_azimuth_annotate",
            "move_data_salmon",
            "convert_for_ui",
            "convert_for_ui_2",
        ],
        cwl_workflows=cwl_workflows_files_salmon,
        no_provenance=True,
        metadata_fun=gather_calculated_metadata,
    )

    send_status_msg_multiome = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=[
            "pipeline_exec",
            "pipeline_exec_azimuth_annotate",
            "move_data_multiome",
            "convert_for_ui",
            "convert_for_ui_2",
        ],
        cwl_workflows=cwl_workflows_files_multiome,
        no_provenance=True,
        metadata_fun=gather_calculated_metadata,
    )

    build_provenance_salmon = build_provenance_function(
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd3"
        ),
    )

    build_provenance_multiome = build_provenance_function(
        cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
            key="cwl_workflows", task_ids="build_cmd2"
        ),
    )

    t_build_provenance_salmon = PythonOperator(
        task_id="build_provenance_salmon",
        python_callable=build_provenance_salmon,
        provide_context=True,
    )

    t_build_provenance_multiome = PythonOperator(
        task_id="build_provenance_multiome",
        python_callable=build_provenance_multiome,
        provide_context=True,
    )

    t_send_status_salmon = PythonOperator(
        task_id="send_status_msg_salmon",
        python_callable=send_status_msg_salmon,
        provide_context=True,
    )
    t_send_status_multiome = PythonOperator(
        task_id="send_status_msg_multiome",
        python_callable=send_status_msg_multiome,
        provide_context=True,
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_move_data_salmon = MoveDataOperator(task_id="move_data_salmon")
    t_move_data_multiome = MoveDataOperator(task_id="move_data_multiome")
    t_join_salmon = JoinOperator(task_id="join_salmon", trigger_rule="one_success")
    t_join_multiome = JoinOperator(task_id="join_multiome", trigger_rule="one_success")
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
        >> t_pipeline_exec_azimuth_annotate
        >> t_maybe_keep_cwl1

        >> prepare_cwl2
        >> t_build_cmd2
        >> t_convert_for_ui
        >> t_maybe_keep_cwl2
        >> t_maybe_skip_cwl3

        >> prepare_cwl3
        >> t_build_cmd3
        >> t_convert_for_ui_2
        >> t_maybe_keep_cwl3
        >> t_move_data_salmon
        >> t_build_provenance_salmon
        >> t_send_status_salmon
        >> t_join_salmon
    )
    (
        t_maybe_skip_cwl3
        >> t_move_data_multiome
        >> t_build_provenance_multiome
        >> t_send_status_multiome
        >> t_join_multiome
        >> t_cleanup_tmpdir
    )
    t_maybe_keep_cwl1 >> t_set_dataset_error
    t_maybe_keep_cwl2 >> t_set_dataset_error
    t_maybe_keep_cwl3 >> t_set_dataset_error
    t_set_dataset_error >> t_join_salmon
    t_join_salmon >> t_cleanup_tmpdir
