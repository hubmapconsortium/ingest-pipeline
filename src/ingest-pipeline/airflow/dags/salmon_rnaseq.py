from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
    SetDatasetProcessingOperator,
    MoveDataOperator,
)

import utils
from utils import (
    get_absolute_workflows,
    get_cwltool_base_cmd,
    get_dataset_uuid,
    get_parent_dataset_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
)

# to be used by the CWL worker
THREADS = 6


def generate_salmon_rnaseq_dag(
    dag_id: str,
    pipeline_name: str,
    assay: str,
    dataset_type: str,
) -> DAG:
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
        dag_id,
        schedule_interval=None,
        is_paused_upon_creation=False,
        default_args=default_args,
        max_active_runs=4,
        user_defined_macros={"tmp_dir_path": utils.get_tmp_dir_path},
    ) as dag:

        cwl_workflows = get_absolute_workflows(
            Path("salmon-rnaseq", "pipeline.cwl"),
            Path("portal-containers", "h5ad-to-arrow.cwl"),
        )

        def build_dataset_name(**kwargs):
            id_l = kwargs["dag_run"].conf["parent_submission_id"]
            inner_str = id_l if isinstance(id_l, str) else "_".join(id_l)
            return f"{dag.dag_id}__{inner_str}__{pipeline_name}"

        prepare_cwl1 = DummyOperator(task_id="prepare_cwl1")

        prepare_cwl2 = DummyOperator(task_id="prepare_cwl2")

        def build_cwltool_cmd1(**kwargs):
            ctx = kwargs["dag_run"].conf
            run_id = kwargs["run_id"]
            tmpdir = utils.get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            data_dirs = ctx["parent_lz_path"]
            data_dirs = [data_dirs] if isinstance(data_dirs, str) else data_dirs
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
                assay,
                "--threads",
                THREADS,
            ]
            for data_dir in data_dirs:
                command.append("--fastq_dir")
                command.append(data_dir)

            return join_quote_command_str(command)

        def build_cwltool_cmd2(**kwargs):
            ctx = kwargs["dag_run"].conf
            run_id = kwargs["run_id"]
            tmpdir = utils.get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)
            data_dir = ctx["parent_lz_path"]
            print("data_dir: ", data_dir)

            command = [
                *get_cwltool_base_cmd(tmpdir),
                cwl_workflows[1],
                "--input_dir",
                ".",
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

        t_pipeline_exec = BashOperator(
            task_id="pipeline_exec",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
            echo $?
            """,
        )

        t_make_arrow1 = BashOperator(
            task_id="make_arrow1",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
            cd "$tmp_dir"/cwl_out/cluster-marker-genes ; \
            {{ti.xcom_pull(task_ids='build_cmd2')}} >> $tmp_dir/session.log 2>&1 ; \
            echo $?
            """,
        )

        t_maybe_keep_cwl1 = BranchPythonOperator(
            task_id="maybe_keep_cwl1",
            python_callable=utils.pythonop_maybe_keep,
            provide_context=True,
            op_kwargs={
                "next_op": "move_files",
                "bail_op": "set_dataset_error",
                "test_op": "pipeline_exec",
            },
        )

        t_maybe_keep_cwl2 = BranchPythonOperator(
            task_id="maybe_keep_cwl2",
            python_callable=utils.pythonop_maybe_keep,
            provide_context=True,
            op_kwargs={
                "next_op": "move_data",
                "bail_op": "set_dataset_error",
                "test_op": "make_arrow1",
            },
        )

        t_send_create_dataset = PythonOperator(
            task_id="send_create_dataset",
            python_callable=utils.pythonop_send_create_dataset,
            provide_context=True,
            op_kwargs={
                "parent_dataset_uuid_callable": get_parent_dataset_uuid,
                "http_conn_id": "ingest_api_connection",
                "endpoint": "/datasets/derived",
                "dataset_name_callable": build_dataset_name,
                "dataset_types": [dataset_type],
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
                "endpoint": "/datasets/status",
                "ds_state": "Error",
                "message": "An error occurred in {}".format(pipeline_name),
            },
        )

        t_move_files = BashOperator(
            task_id="move_files",
            bash_command="""
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            cd "$tmp_dir"/cwl_out ; \
            mkdir cluster-marker-genes ; \
            mv cluster_marker_genes.h5ad cluster-marker-genes ; \
            echo $?
            """,
        )

        send_status_msg = make_send_status_msg_function(
            dag_file=__file__,
            retcode_ops=["pipeline_exec", "move_data", "make_arrow1"],
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
            >> t_move_files
            >> prepare_cwl2
            >> t_build_cmd2
            >> t_make_arrow1
            >> t_maybe_keep_cwl2
            >> t_move_data
            >> t_send_status
            >> t_join
        )
        t_maybe_keep_cwl1 >> t_set_dataset_error
        t_maybe_keep_cwl2 >> t_set_dataset_error
        t_set_dataset_error >> t_join
        t_join >> t_cleanup_tmpdir

    return dag


# dag_id, pipeline name, assay given to pipeline via --assay, dataset type
salmon_dag_data: List[Tuple[str, str, str, str]] = [
    (
        "salmon_rnaseq_10x",
        "salmon-rnaseq",
        "10x",
        "salmon_rnaseq_10x",
    ),
    (
        "salmon_rnaseq_sciseq",
        "salmon-rnaseq-sciseq",
        "sciseq",
        "salmon_rnaseq_sciseq",
    ),
    (
        "salmon_rnaseq_slideseq",
        "salmon-rnaseq-slideseq",
        "slideseq",
        "salmon_rnaseq_slideseq",
    ),
    (
        "salmon_rnaseq_snareseq",
        "salmon-rnaseq-snareseq",
        "snareseq",
        "salmon_rnaseq_snareseq",
    ),
]

for dag_id, pipeline_name, assay, dataset_type in salmon_dag_data:
    globals()[dag_id] = generate_salmon_rnaseq_dag(
        dag_id=dag_id,
        pipeline_name=pipeline_name,
        assay=assay,
        dataset_type=dataset_type,
    )
