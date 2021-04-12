from datetime import datetime, timedelta
from typing import List

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
from airflow.hooks.http_hook import HttpHook

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

def generate_cells_index_dag(modality:str):

    def get_dataset_uuids(modality: str) -> List[str]:
        hits = []
        for i in range(50):
            dataset_query_dict = {
                "from": 10 * i,
                "size": 10,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_phrase": {
                                    "status": {
                                        "query": "Published"
                                    }
                                }
                            }
                        ],
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
                        "must_not": [],
                    }
                }
            }

            http_conn_id = "search_api_connection"
            http_hook = HttpHook(method="POST", http_conn_id=http_conn_id)

            dataset_response = http_hook.run('/search/', json=dataset_query_dict)
            hits.extend(dataset_response.json()['hits']['hits'])

        print(len(hits))

        uuids = []
        for hit in hits:
            for ancestor in hit['_source']['ancestors']:
                if 'data_types' in ancestor.keys():
                    print('data_types_found')
                    if modality in ancestor['data_types'][0] and 'bulk' not in ancestor['data_types'][0]:
                        uuids.append(hit['_source']['uuid'])

        return set(uuids)

    def get_all_indexed_datasets()->List[str]:
        http_conn_id = "cells_api_connection"
        http_hook = HttpHook(method="POST", http_conn_id=http_conn_id)
        query_handle = http_hook.run('/dataset/', {}).json()['results'][0]['query_handle']
        num_datasets = http_hook.run('/count/', {'key':query_handle, 'set_type':'dataset'})
        evaluation_args_dict = {'key':query_handle, 'set_type':'dataset', 'limit':num_datasets}
        datasets_response = http_hook.run('/datasetevaluation/', evaluation_args_dict)
        datasets_results = datasets_response.json()['results']
        uuids = [result['uuid'] for result in datasets_results]
        return set(uuids)

    def uuid_to_abs_path(uuid:str)->str:
        http_conn_id = "ingest_api_connection"
        http_hook = HttpHook(method="GET", http_conn_id=http_conn_id)
        endpoint = f"datasets/{uuid}/file-system-abs-path"
        return http_hook.run(endpoint).json()['path']

    def get_data_directories(modality:str)->List[str]:
        all_indexed_datasets = get_all_indexed_datasets()
        modality_datasets = get_dataset_uuids(modality)
        unindexed_datasets = modality_datasets - all_indexed_datasets
        if len(unindexed_datasets) > 0:
            return [uuid_to_abs_path(dataset) for dataset in modality_datasets]
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
        f"cells_index_{modality}",
        schedule_interval=None,
        is_paused_upon_creation=False,
        default_args=default_args,
        max_active_runs=4,
        user_defined_macros={"tmp_dir_path": utils.get_tmp_dir_path},
    ) as dag:
            repo_name = "cross-dataset-diffexpr" if modality == "rna" else f"cross-dataset-{modality}"
            workflow_name = f"cross-dataset-{modality}.cwl"
            cwl_workflows = get_absolute_workflows(repo_name, workflow_name)

            prepare_cwl = DummyOperator(task_id=f"prepare_cwl-{modality}")

            def build_cwltool_cmd(**kwargs):
                run_id = kwargs["run_id"]
                tmpdir = utils.get_tmp_dir_path(run_id)
                print("tmpdir: ", tmpdir)

                data_dirs = get_data_directories(modality)
                if len(data_dirs) == 0:
                    raise ValueError("No new data directories found")
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

                return join_quote_command_str(command)

            t_build_cmd = PythonOperator(
                task_id=f"build_cmd_{modality}",
                python_callable=build_cwltool_cmd,
                provide_context=True,
            )

            t_maybe_run_pipeline = BranchPythonOperator(
                task_id=f"maybe_run_pipeline_{modality}",
                python_callable=utils.pythonop_maybe_keep,
                provide_context=True,
                op_kwargs={
                    "next_op": f"pipeline_exec_{modality}",
                    "bail_op": f"join_{modality}",
                    "test_op": f"build_cmd_{modality}",
                },
            )

            t_pipeline_exec = BashOperator(
                task_id=f"pipeline_exec_{modality}",
                bash_command=""" \
                tmp_dir={{tmp_dir_path(run_id)}} ; \
                {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
                echo $?
                """,
            )

            t_maybe_keep_cwl = BranchPythonOperator(
                task_id=f"maybe_keep_cwl_{modality}",
                python_callable=utils.pythonop_maybe_keep,
                provide_context=True,
                op_kwargs={
                    "next_op": f"prepare_cwl_{modality}",
                    "bail_op": f"set_dataset_error_{modality}",
                    "test_op": f"pipeline_exec_{modality}",
                },
            )
            t_log_info = LogInfoOperator(task_id=f"log_info_{modality}")
            t_join = JoinOperator(task_id=f"join_{modality}")
            t_create_tmpdir = CreateTmpDirOperator(task_id=f"create_tmpdir_{modality}")
            t_cleanup_tmpdir = CleanupTmpDirOperator(task_id=f"cleanup_tmpdir_{modality}")
            t_move_data = MoveDataOperator(task_id=f"move_data_{modality}")

            dag >> t_log_info >> t_create_tmpdir
            (
                t_create_tmpdir
                >> prepare_cwl
                >> t_build_cmd
                >> t_maybe_run_pipeline
                >> t_pipeline_exec
                >> t_maybe_keep_cwl
                >> t_move_data
            )
            t_maybe_keep_cwl >> t_join
            t_maybe_run_pipeline >> t_join
            t_move_data >> t_join >> t_cleanup_tmpdir

modalities = ["rna", "atac", "codex"]

for modality in modalities:
    generate_cells_index_dag(modality)