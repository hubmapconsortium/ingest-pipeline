from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
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
    SequencingDagParameters,
    get_absolute_workflows,
    get_cwltool_base_cmd,
    get_dataset_uuid,
    get_parent_dataset_uuids_list,
    get_parent_data_dirs_list,
    build_dataset_name as inner_build_dataset_name,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
)

# to be used by the CWL worker
THREADS = 6


def generate_salmon_rnaseq_dag(params: SequencingDagParameters) -> DAG:
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
        "queue": utils.map_queue_name("general"),
        "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
    }

    with DAG(
        params.dag_id,
        schedule_interval=None,
        is_paused_upon_creation=False,
        default_args=default_args,
        max_active_runs=4,
        user_defined_macros={"tmp_dir_path": get_tmp_dir_path},
    ) as dag:

        cwl_workflows = get_absolute_workflows(
            Path("salmon-rnaseq", "pipeline.cwl"),
            Path("portal-containers", "h5ad-to-arrow.cwl"),
            Path("portal-containers", "anndata-to-ui.cwl"),
        )

        def build_dataset_name(**kwargs):
            return inner_build_dataset_name(dag.dag_id, params.pipeline_name, **kwargs)

        prepare_cwl1 = DummyOperator(task_id="prepare_cwl1")

        prepare_cwl2 = DummyOperator(task_id="prepare_cwl2")

        prepare_cwl3 = DummyOperator(task_id="prepare_cwl3")

        def build_cwltool_cmd1(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            data_dirs = get_parent_data_dirs_list(**kwargs)
            print("data_dirs: ", data_dirs)

            command = [
                *get_cwltool_base_cmd(tmpdir),
                "--relax-path-checks",
                "--debug",
                "--outdir",
                tmpdir / "cwl_out",
                "--parallel",
                cwl_workflows[0],
                "--assay",
                params.assay,
                "--threads",
                THREADS,
            ]
            for data_dir in data_dirs:
                command.append("--fastq_dir")
                command.append(data_dir)

            return join_quote_command_str(command)

        def build_cwltool_cmd2(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            command = [
                *get_cwltool_base_cmd(tmpdir),
                cwl_workflows[1],
                "--input_dir",
                # This pipeline invocation runs in a 'hubmap_ui' subdirectory,
                # so use the parent directory as input
                "..",
            ]

            return join_quote_command_str(command)

        def build_cwltool_cmd3(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            command = [
                *get_cwltool_base_cmd(tmpdir),
                cwl_workflows[2],
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

        t_build_cmd3 = PythonOperator(
            task_id="build_cmd3",
            python_callable=build_cwltool_cmd3,
            provide_context=True,
        )

        t_pipeline_exec = BashOperator(
            task_id="pipeline_exec",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
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
                "dataset_name_callable": build_dataset_name,
                "dataset_types": [params.dataset_type],
            },
        )

        t_set_dataset_error = PythonOperator(
            task_id="set_dataset_error",
            python_callable=utils.pythonop_set_dataset_state,
            provide_context=True,
            trigger_rule="all_done",
            op_kwargs={
                "dataset_uuid_callable": get_dataset_uuid,
                "http_conn_id": "ingest_api_connection",
                "ds_state": "Error",
                "message": f"An error occurred in {params.pipeline_name}",
            },
        )

        send_status_msg = make_send_status_msg_function(
            dag_file=__file__,
            retcode_ops=["pipeline_exec",
                         "move_data",
                         "convert_for_ui",
                         "convert_for_ui_2"],
            cwl_workflows=cwl_workflows,
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
        t_set_dataset_processing = SetDatasetProcessingOperator(task_id="set_dataset_processing")
        t_move_data = MoveDataOperator(task_id="move_data")

        (
            dag
            >> t_log_info
            >> t_create_tmpdir
            >> t_send_create_dataset
            >> t_set_dataset_processing
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
            >> t_move_data
            >> t_send_status
            >> t_join
        )
        t_maybe_keep_cwl1 >> t_set_dataset_error
        t_maybe_keep_cwl2 >> t_set_dataset_error
        t_maybe_keep_cwl3 >> t_set_dataset_error
        t_set_dataset_error >> t_join
        t_join >> t_cleanup_tmpdir

    return dag


def get_salmon_dag_params(assay: str) -> SequencingDagParameters:
    # TODO: restructure assay names, pipeline names, etc.; this repetition
    #   is for backward compatibility
    return SequencingDagParameters(
        dag_id=f"salmon_rnaseq_{assay}",
        pipeline_name=f"salmon-rnaseq-{assay}",
        assay=assay,
        dataset_type=f"salmon_rnaseq_{assay}",
    )


salmon_dag_params: List[SequencingDagParameters] = [
    # 10X is special because it was first; no "10x" label in the pipeline name
    SequencingDagParameters(
        dag_id="salmon_rnaseq_10x",
        pipeline_name="salmon-rnaseq",
        assay="10x_v3",
        dataset_type="salmon_rnaseq_10x",
    ),
    SequencingDagParameters(
        dag_id="salmon_rnaseq_10x_sn",
        pipeline_name="salmon-rnaseq",
        assay="10x_v3_sn",
        dataset_type="salmon_sn_rnaseq_10x",
    ),
    SequencingDagParameters(
        dag_id="salmon_rnaseq_10x_v2",
        pipeline_name="salmon-rnaseq",
        assay="10x_v2",
        dataset_type="salmon_rnaseq_10x",
    ),
    SequencingDagParameters(
        dag_id="salmon_rnaseq_10x_v2_sn",
        pipeline_name="salmon-rnaseq",
        assay="10x_v2_sn",
        dataset_type="salmon_sn_rnaseq_10x",
    ),
    get_salmon_dag_params("sciseq"),
    get_salmon_dag_params("slideseq"),
    get_salmon_dag_params("snareseq"),
]

for params in salmon_dag_params:
    globals()[params.dag_id] = generate_salmon_rnaseq_dag(params)
