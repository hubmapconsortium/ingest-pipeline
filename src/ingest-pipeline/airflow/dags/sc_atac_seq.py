from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from airflow import DAG
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
    SequencingDagParameters,
    get_absolute_workflow,
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
    HMDAG,
    get_queue_resource,
    get_threads_resource,
    get_preserve_scratch_resource,
    get_cwl_cmd_from_workflows,
)


def generate_atac_seq_dag(params: SequencingDagParameters) -> DAG:
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
        "queue": get_queue_resource(params.dag_id),
        "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
    }

    with HMDAG(
        params.dag_id,
        schedule_interval=None,
        is_paused_upon_creation=False,
        default_args=default_args,
        user_defined_macros={
            "tmp_dir_path": get_tmp_dir_path,
            "preserve_scratch": get_preserve_scratch_resource(params.dag_id),
        },
    ) as dag:
        workflow_version = "1.0.0"

        cwl_workflows = [
            {
                "workflow_path": str(
                    get_absolute_workflow(
                        Path("sc-atac-seq-pipeline", "sc_atac_seq_prep_process_analyze.cwl")
                    )
                ),
                "documentation_url": "",
            },
            {
                "workflow_path": str(
                    get_absolute_workflow(Path("portal-containers", "scatac-csv-to-arrow.cwl"))
                ),
                "documentation_url": "",
            },
        ]

        def build_dataset_name(**kwargs):
            return inner_build_dataset_name(dag.dag_id, params.pipeline_name, **kwargs)

        prepare_cwl1 = DummyOperator(task_id="prepare_cwl1")

        prepare_cwl2 = DummyOperator(task_id="prepare_cwl2")

        def build_cwltool_cmd1(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)
            data_dirs = get_parent_data_dirs_list(**kwargs)
            print("data_dirs: ", data_dirs)

            cwl_params = [
                {"parameter_name": "--parallel", "value": ""},
            ]
            input_parameters = [
                {"parameter_name": "--assay", "value": params.assay},
                {"parameter_name": "--exclude_bam", "value": ""},
                {"parameter_name": "--threads", "value": get_threads_resource(dag.dag_id)},
                {
                    "parameter_name": "--sequence_directory",
                    "value": [str(data_dir) for data_dir in data_dirs],
                },
            ]

            command = get_cwl_cmd_from_workflows(
                cwl_workflows, 0, input_parameters, tmpdir, kwargs["ti"], cwl_params
            )

            return join_quote_command_str(command)

        def build_cwltool_cmd2(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd1")

            input_parameters = [
                {"parameter_name": "--input_dir", "value": str(tmpdir / "cwl_out")},
            ]
            command = get_cwl_cmd_from_workflows(
                workflows, 1, input_parameters, tmpdir, kwargs["ti"]
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

        t_pipeline_exec = BashOperator(
            task_id="pipeline_exec",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            mkdir -p ${tmp_dir}/cwl_out ; \
            {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
            echo $?
            """,
        )

        t_make_arrow1 = BashOperator(
            task_id="make_arrow1",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
            {{ti.xcom_pull(task_ids='build_cmd2')}} >> $tmp_dir/session.log 2>&1 ; \
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
                "next_op": "maybe_create_dataset",
                "bail_op": "set_dataset_error",
                "test_op": "make_arrow1",
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
                "pipeline_shorthand": "ArchR",
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
                "message": f"An error occurred in {params.pipeline_name}",
            },
        )

        send_status_msg = make_send_status_msg_function(
            dag_file=__file__,
            retcode_ops=["pipeline_exec", "move_data", "make_arrow1"],
            cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
                key="cwl_workflows", task_ids="build_cmd2"
            ),
            workflow_description=params.workflow_description,
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
            >> t_make_arrow1
            >> t_maybe_keep_cwl2
            >> t_maybe_create_dataset

            >> t_send_create_dataset
            >> t_move_data
            >> t_send_status
            >> t_join
        )
        t_maybe_keep_cwl1 >> t_set_dataset_error
        t_maybe_keep_cwl2 >> t_set_dataset_error
        t_set_dataset_error >> t_join
        t_maybe_create_dataset >> t_join
        t_join >> t_cleanup_tmpdir

    return dag


atacseq_dag_data: List[SequencingDagParameters] = [
    SequencingDagParameters(
        dag_id="sc_atac_seq_sci",
        pipeline_name="sci-atac-seq-pipeline",
        assay="sciseq",
        workflow_description="The pipeline for multiome sciATACseq data uses HISAT2 for short read alignment of ATACseq reads HG38 reference genome and ArchR to convert the resulting BAM file to a cell by bin matrix, which is used to calculate TSS enrichment, differential enrichment of transcription factors, perform clustering of nuclei and additional analysis.",
    ),
    SequencingDagParameters(
        dag_id="sc_atac_seq_snare",
        pipeline_name="sc-atac-seq-pipeline",
        assay="snareseq",
        workflow_description="The pipeline for multiome RNA-ATACseq data uses Salmon for alignment free quasi mapping of reads from RNA sequencing to the HG38 reference genome and HISAT2 for short read alignment of ATACseq reads to the same genome.  Barcodes are then mapped between components of the assay to generate an annotated data matrix with consolidated RNA and ATACseq data.  This annotated data matrix is then passed to the Muon package for dimensionality reduction, clustering, and multiomic factor analysis.  Cell type annotations are provided by Azimuth when available for the type of tissue being analyzed.",
    ),
    SequencingDagParameters(
        dag_id="sc_atac_seq_sn",
        pipeline_name="sn-atac-seq-pipeline",
        assay="snseq",
        workflow_description="Thee pipeline for snATACseq data uses HISAT2 for short read alignment of ATACseq reads HG38 reference genome and ArchR to convert the resulting BAM file to a cell by bin matrix, which is used to calculate TSS enrichment, differential enrichment of transcription factors, perform clustering of nuclei and additional analysis.",
    ),
    SequencingDagParameters(
        dag_id="sc_atac_seq_multiome_10x",
        pipeline_name="sn-atac-seq-pipeline",
        assay="multiome_10x",
        workflow_description="The pipeline for multiome RNA-ATACseq data uses Salmon for alignment free quasi mapping of reads from RNA sequencing to the HG38 reference genome and HISAT2 for short read alignment of ATACseq reads to the same genome.  Barcodes are then mapped between components of the assay to generate an annotated data matrix with consolidated RNA and ATACseq data.  This annotated data matrix is then passed to the Muon package for dimensionality reduction, clustering, and multiomic factor analysis.  Cell type annotations are provided by Azimuth when available for the type of tissue being analyzed.",
    ),
]

for params in atacseq_dag_data:
    globals()[params.dag_id] = generate_atac_seq_dag(params)
