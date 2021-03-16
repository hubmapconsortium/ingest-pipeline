from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple
import requests


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
)

import utils
from utils import (
    get_absolute_workflows,
    get_cwltool_base_cmd,
    get_dataset_uuid,
    get_uuid_for_error,
    join_quote_command_str,
)

# to be used by the CWL worker
THREADS = 6

def get_dataset_uuids(modality: str) -> List[str]:
    hits = []
    for i in range(50):
        dataset_query_dict = {
            "from": 10 * i,
            "size": 10,
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "match_all": {}
                        },
                        {
                            "exists": {
                                "field": "files.rel_path"
                            }
                        },
                        {
                            "match_phrase": {
                                "immediate_ancestors.entity_type": {
                                    "query": "Dataset"
                                }
                            }
                        },
                    ],
                    "should": [],
                    "must_not": [
                        {
                            "match_phrase": {
                                "status": {
                                    "query": "Error"
                                }
                            }
                        }
                    ]
                }
            }
        }

        dataset_response = requests.post(
            'https://search.api.hubmapconsortium.org/search',
            json=dataset_query_dict)
        hits.extend(dataset_response.json()['hits']['hits'])

    print(len(hits))

    uuids = []
    for hit in hits:
        for ancestor in hit['_source']['ancestors']:
            if 'data_types' in ancestor.keys():
                print('data_types_found')
                if modality in ancestor['data_types'][0] and 'bulk' not in ancestor['data_types'][0]:
                    uuids.append(hit['_source']['uuid'])

    return uuids

def get_all_indexed_datasets()->List[str]:
    cells_url = 'https://cells.api.hubmapconsortium.org/api/'
    query_handle = requests.post(cells_url + 'dataset/', {}).json()['results'][0]['query_handle']
    num_datasets = requests.post(cells_url + 'count/', {'key':query_handle, 'set_type':'dataset'})
    evaluation_args_dict = {'key':query_handle, 'set_type':'dataset', 'limit':num_datasets}
    datasets_response = requests.post(cells_url + 'datasetevaluation/', evaluation_args_dict)
    datasets_results = datasets_response.json()['results']
    uuids = [result['uuid'] for result in datasets_results]
    return uuids

def get_data_directories(modality):
    all_indexed_datasets = get_all_indexed_datasets()
    modality_datasets = get_dataset_uuids(modality)
    if any([uuid not in all_indexed_datasets for uuid in modality_datasets]):
        return [f"/hive/hubmap/data/public/{dataset}" for dataset in modality_datasets]
    else:
        return []

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
    "cells_index",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    max_active_runs=4,
    user_defined_macros={"tmp_dir_path": utils.get_tmp_dir_path},
) as dag:

    cwl_workflows = get_absolute_workflows(
        Path("cross-dataset-atac", "cross-dataset-atac.cwl"),
        Path("cross-dataset-diffexpr", "cross-dataset-rna.cwl"),
        Path("cross-dataset-codex", "cross-dataset-codex.cwl"),
    )

    prepare_cwl1 = DummyOperator(task_id="prepare_cwl1")

    prepare_cwl2 = DummyOperator(task_id="prepare_cwl2")

    prepare_cwl3 = DummyOperator(task_id="prepare_cwl3")

    def build_cwltool_cmd1(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = utils.get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        data_dirs = get_data_directories("atac")
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
        ]

        for data_dir in data_dirs:
            command.append("--data_directories")
            command.append(data_dir)

        return join_quote_command_str(command) if len(data_dirs) > 0 else ""

    def build_cwltool_cmd2(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = utils.get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        data_dirs = get_data_directories("rna")
        data_dirs = [data_dirs] if isinstance(data_dirs, str) else data_dirs
        print("data_dirs: ", data_dirs)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            "--relax-path-checks",
            "--debug",
            "--outdir",
            tmpdir / "cwl_out",
            "--parallel",
            cwl_workflows[1],
        ]

        for data_dir in data_dirs:
            command.append("--data_directories")
            command.append(data_dir)

        return join_quote_command_str(command) if len(data_dirs) > 0 else ""

    def build_cwltool_cmd3(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = utils.get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        data_dirs = get_data_directories("codex")
        data_dirs = [data_dirs] if isinstance(data_dirs, str) else data_dirs
        print("data_dirs: ", data_dirs)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            "--relax-path-checks",
            "--debug",
            "--outdir",
            tmpdir / "cwl_out",
            "--parallel",
            cwl_workflows[2],
        ]

        for data_dir in data_dirs:
            command.append("--data_directories")
            command.append(data_dir)

        return join_quote_command_str(command) if len(data_dirs) > 0 else ""

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

    t_pipeline_exec1 = BashOperator(
        task_id="pipeline_exec",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_pipeline_exec2 = BashOperator(
        task_id="pipeline_exec",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_pipeline_exec3 = BashOperator(
        task_id="pipeline_exec",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
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


    t_log_info = LogInfoOperator(task_id="log_info")
    t_join = JoinOperator(task_id="join")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")
    t_move_data = MoveDataOperator(task_id="move_data")

    (
        dag
        >> t_log_info
        >> t_create_tmpdir
        >> prepare_cwl1
        >> t_build_cmd1
        >> t_pipeline_exec1
        >> t_maybe_keep_cwl1
        >> prepare_cwl2
        >> t_build_cmd2
        >> t_pipeline_exec2
        >> t_maybe_keep_cwl2
        >> prepare_cwl3
        >> t_build_cmd3
        >> t_pipeline_exec3
        >> t_maybe_keep_cwl3
        >> t_move_data
        >> t_join
    )
    t_maybe_keep_cwl1 >> t_join
    t_maybe_keep_cwl2 >> t_join
    t_maybe_keep_cwl3 >> t_join
    t_join >> t_cleanup_tmpdir
