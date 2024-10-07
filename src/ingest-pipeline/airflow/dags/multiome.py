from collections import namedtuple
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
    pythonop_get_dataset_state,
    HMDAG,
    get_queue_resource,
    get_threads_resource,
    get_preserve_scratch_resource,
)

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
        cwl_workflows = get_absolute_workflows(
            Path("multiome-rna-atac-pipeline", "pipeline.cwl"),
            Path("azimuth-annotate", "pipeline.cwl"),
            Path("portal-containers", "mudata-to-ui.cwl"),
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

            source_type = ""
            unique_source_types = set()
            for parent_uuid in get_parent_dataset_uuids_list(**kwargs):
                dataset_state = pythonop_get_dataset_state(
                    dataset_uuid_callable=lambda **kwargs: parent_uuid, **kwargs)
                source_type = dataset_state.get("source_type")
                if source_type == "mixed":
                    print("Force failure. Should only be one unique source_type for a dataset.")
                else:
                    unique_source_types.add(source_type)

            if len(unique_source_types) > 1:
                print("Force failure. Should only be one unique source_type for a dataset.")
            else:
                source_type = unique_source_types.pop().lower()

            command = [
                *get_cwltool_base_cmd(tmpdir),
                "--outdir",
                tmpdir / "cwl_out",
                "--parallel",
                cwl_workflows[0],
                "--threads_rna",
                get_threads_resource(dag.dag_id),
                "--threads_atac",
                get_threads_resource(dag.dag_id),
                "--organism",
                source_type
            ]

            for component in ["RNA", "ATAC"]:
                command.append(f"--assay_{component.lower()}")
                command.append(getattr(params, f"assay_{component.lower()}"))
                for data_dir in data_dirs:
                    command.append(f"--fastq_dir_{component.lower()}")
                    command.append(data_dir / Path(f"raw/fastq/{component}"))

            atac_metadata_files = [find_atac_metadata_file(data_dir) for data_dir in data_dirs]
            if params.requires_one_atac_metadata_file:
                if (count := len(atac_metadata_files)) != 1:
                    raise ValueError(f"Need 1 ATAC-seq metadata file, found {count}")
                command.append("--atac_metadata_file")
                command.append(atac_metadata_files[0])

            return join_quote_command_str(command)

        def build_cwltool_cmd2(**kwargs):
            run_id = kwargs["run_id"]
            tmpdir = get_tmp_dir_path(run_id)
            print("tmpdir: ", tmpdir)

            # get organ type
            ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=get_dataset_uuid, **kwargs)

            organ_list = list(set(ds_rslt["organs"]))
            organ_code = organ_list[0] if len(organ_list) == 1 else "multi"

            command = [
                *get_cwltool_base_cmd(tmpdir),
                cwl_workflows[1],
                "--reference",
                organ_code,
                "--matrix",
                "mudata_raw.h5mu",
                "--secondary-analysis-matrix",
                "secondary_analysis.h5mu",
                "--assay",
                params.assay_azimuth,
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

        t_pipeline_exec_azimuth_annotate = BashOperator(
            task_id="pipeline_exec_azimuth_annotate",
            bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            cd "$tmp_dir"/cwl_out ; \
            {{ti.xcom_pull(task_ids='build_cmd2')}} >> $tmp_dir/session.log 2>&1 ; \
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
                "next_op": "move_data",
                "bail_op": "set_dataset_error",
                "test_op": "convert_for_ui",
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
            t_log_info
            >> t_create_tmpdir
            >> t_send_create_dataset
            >> t_set_dataset_processing
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
