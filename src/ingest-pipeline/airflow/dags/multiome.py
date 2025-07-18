from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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
    get_parent_data_dirs_list,
    build_dataset_name as inner_build_dataset_name,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    pythonop_get_dataset_state,
    HMDAG,
    get_queue_resource,
    get_threads_resource,
    get_preserve_scratch_resource,
    get_cwl_cmd_from_workflows,
    gather_calculated_metadata,
)

from extra_utils import build_tag_containers

MultiomeSequencingDagParameters = namedtuple(
    "MultiomeSequencingDagParameters",
    [
        "dag_id",
        "pipeline_name",
        "assay_rna",
        "assay_atac",
        "assay_azimuth",
        "requires_one_atac_metadata_file",
    ],
)


def find_atac_metadata_file(data_dir: Path) -> Path:
    for path in data_dir.glob("*.tsv"):
        name_lower = path.name.lower()
        if path.is_file() and "atac" in name_lower and "metadata" in name_lower:
            return path
    raise ValueError("Couldn't find ATAC-seq metadata file")


def generate_multiome_dag(params: MultiomeSequencingDagParameters) -> DAG:
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
        workflow_description = "The pipeline for multiome RNA-ATACseq data uses Salmon for alignment free quasi mapping of reads from RNA sequencing to the hg38 reference genome and HISAT2 for short read alignment of ATACseq reads to the same genome.  Barcodes are then mapped between components of the assay to generate an annotated data matrix with consolidated RNA and ATACseq data.  This annotated data matrix is then passed to the Muon package for dimensionality reduction, clustering, and multiomic factor analysis.  Cell type annotations are provided by Azimuth when available for the type of tissue being analyzed."

        cwl_workflows = [
            {
                "workflow_path": str(
                    get_absolute_workflow(Path("multiome-rna-atac-pipeline", "pipeline.cwl"))
                ),
                "documentation_url": "",
            },
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

        def build_dataset_name(**kwargs):
            return inner_build_dataset_name(dag.dag_id, params.pipeline_name, **kwargs)

        @task(task_id="prepare_cwl1")
        def prepare_cwl_cmd1(**kwargs):
            if kwargs["dag_run"].conf.get("dryrun"):
                cwl_path = Path(cwl_workflows[0]["workflow_path"]).parent
                return build_tag_containers(cwl_path)
            else:
                return "No Container build required"

        prepare_cwl1 = prepare_cwl_cmd1()

        prepare_cwl2 = EmptyOperator(task_id="prepare_cwl2")

        prepare_cwl3 = EmptyOperator(task_id="prepare_cwl3")

        def build_cwltool_cmd1(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            data_dirs = get_parent_data_dirs_list(**kwargs)
            print("data_dirs: ", data_dirs)

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

            cwl_params = [
                {"parameter_name": "--parallel", "value": ""},
            ]

            input_parameters = [
                {"parameter_name": "--threads_rna", "value": get_threads_resource(dag.dag_id)},
                {"parameter_name": "--threads_atac", "value": get_threads_resource(dag.dag_id)},
                {"parameter_name": "--organism", "value": source_type},
                {"parameter_name": "--assay_rna", "value": params.assay_rna},
                {
                    "parameter_name": "--fastq_dir_rna",
                    "value": [str(data_dir / Path(f"raw/fastq/RNA")) for data_dir in data_dirs],
                },
                {"parameter_name": "--assay_atac", "value": params.assay_atac},
                {
                    "parameter_name": "--fastq_dir_atac",
                    "value": [str(data_dir / Path(f"raw/fastq/ATAC")) for data_dir in data_dirs],
                },
            ]

            atac_metadata_files = [find_atac_metadata_file(data_dir) for data_dir in data_dirs]
            if params.requires_one_atac_metadata_file:
                if (count := len(atac_metadata_files)) != 1:
                    raise ValueError(f"Need 1 ATAC-seq metadata file, found {count}")
                input_parameters.append(
                    {
                        "parameter_name": "--atac_metadata_file",
                        "value": str(atac_metadata_files[0]),
                    }
                )

            command = get_cwl_cmd_from_workflows(
                cwl_workflows, 0, input_parameters, tmpdir, kwargs["ti"], cwl_params
            )

            return join_quote_command_str(command)

        def build_cwltool_cmd2(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            # get organ type
            ds_rslt = pythonop_get_dataset_state(
                dataset_uuid_callable=lambda **kwargs: get_parent_dataset_uuids_list(**kwargs)[0],
                **kwargs,
            )

            source_type = ds_rslt.get("source_type", "human")
            if source_type == "mixed":
                print("Force failure. Should only be one unique source_type for a dataset.")

            workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd1")

            input_parameters = [
                {
                    "parameter_name": "--secondary-analysis-matrix",
                    "value": str(tmpdir / "cwl_out/secondary_analysis.h5ad"),
                },
                {
                    "parameter_name": "--source",
                    "value": source_type,
                },
            ]
            command = get_cwl_cmd_from_workflows(
                workflows, 1, input_parameters, tmpdir, kwargs["ti"]
            )

            return join_quote_command_str(command)

        def build_cwltool_cmd3(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            workflows = kwargs["ti"].xcom_pull(key="cwl_workflows", task_ids="build_cmd2")

            cwl_parameters = [
                {"parameter_name": "--outdir", "value": str(tmpdir / "cwl_out/hubmap_ui")}
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

        t_pipeline_exec = BashOperator(
            task_id="pipeline_exec",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            mkdir -p ${tmp_dir}/cwl_out ; \
            {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
            echo $?
            """,
        )

        t_pipeline_exec_azimuth_annotate = BashOperator(
            task_id="pipeline_exec_azimuth_annotate",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            {{ti.xcom_pull(task_ids='build_cmd2')}} >> $tmp_dir/session.log 2>&1 ; \
            echo $?
            """,
        )

        t_convert_for_ui = BashOperator(
            task_id="convert_for_ui",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
            mkdir -p ${tmp_dir}/cwl_out/hubmap_ui ; \
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
                "test_op": "pipeline_exec_azimuth_annotate",
            },
        )

        t_maybe_keep_cwl3 = BranchPythonOperator(
            task_id="maybe_keep_cwl3",
            python_callable=utils.pythonop_maybe_keep,
            provide_context=True,
            op_kwargs={
                "next_op": "maybe_create_dataset",
                "bail_op": "set_dataset_error",
                "test_op": "convert_for_ui",
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
                "pipeline_shorthand": "Salmon + ArchR + Muon",
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
            retcode_ops=[
                "pipeline_exec",
                "pipeline_exec_azimuth_annotate",
                "move_data",
                "convert_for_ui",
            ],
            cwl_workflows=lambda **kwargs: kwargs["ti"].xcom_pull(
                key="cwl_workflows", task_ids="build_cmd3"
            ),
            workflow_description=workflow_description,
            workflow_version=workflow_version,
            metadata_fun=gather_calculated_metadata,
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
            >> t_pipeline_exec_azimuth_annotate
            >> t_maybe_keep_cwl2

            >> prepare_cwl3
            >> t_build_cmd3
            >> t_convert_for_ui
            >> t_maybe_keep_cwl3
            >> t_maybe_create_dataset

            >> t_send_create_dataset
            >> t_move_data
            >> t_send_status
            >> t_join
        )
        t_maybe_keep_cwl1 >> t_set_dataset_error
        t_maybe_keep_cwl2 >> t_set_dataset_error
        t_maybe_keep_cwl3 >> t_set_dataset_error
        t_set_dataset_error >> t_join
        t_maybe_create_dataset >> t_join
        t_join >> t_cleanup_tmpdir

    return dag


def get_simple_multiome_dag_params(assay: str) -> MultiomeSequencingDagParameters:
    return MultiomeSequencingDagParameters(
        dag_id=f"multiome_{assay}",
        pipeline_name=f"multiome-{assay}",
        assay_rna=assay,
        assay_atac=assay,
        assay_azimuth=assay,
        requires_one_atac_metadata_file=False,
    )


multiome_dag_params: List[MultiomeSequencingDagParameters] = [
    MultiomeSequencingDagParameters(
        dag_id="multiome_10x",
        pipeline_name="multiome-10x",
        assay_rna="multiome_10x",
        assay_atac="multiome_10x",
        assay_azimuth="10x_v3_sn",
        requires_one_atac_metadata_file=True,
    ),
    get_simple_multiome_dag_params("snareseq"),
]

for params in multiome_dag_params:
    globals()[params.dag_id] = generate_multiome_dag(params)
