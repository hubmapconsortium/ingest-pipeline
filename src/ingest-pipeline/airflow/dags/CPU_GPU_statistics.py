import pandas as pd
import re

from airflow.decorators import task
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.configuration import conf as airflow_conf
from pathlib import Path
from typing import List, Dict

from utils import (
    get_queue_resource,
    create_dataset_state_error_callback,
    get_uuid_for_error,
    HMDAG,
    encrypt_tok,
    get_tmp_dir_path,
    get_auth_tok,
)

from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
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
    "queue": get_queue_resource("codex_cytokit"),
    "on_failure_callback": create_dataset_state_error_callback(get_uuid_for_error),
}

with HMDAG(
    "CPU_GPU_statistics",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
    },
) as dag:

    def _get_dataset_base_paths(dataset_ids: List[str], auth_tok: str) -> Dict[str, str]:
        """Get base paths for multiple datasets from Ingest API.

        Args:
            dataset_ids: List of dataset UUIDs to fetch base paths for

        Returns:
            Dict mapping dataset UUID to base path
        """
        headers = {
            "authorization": f"Bearer {auth_tok}",
            "content-type": "application/json",
            "Cache-Control": "no-cache",
            "X-Hubmap-Application": "ingest-pipeline",
        }
        base_paths = {}

        for uuid in dataset_ids:
            endpoint = f"datasets/{uuid}/file-system-abs-path"

            http_hook = HttpHook("GET", http_conn_id="ingest_api_connection")
            response = http_hook.run(
                endpoint, headers=headers, extra_options={"check_response": False}
            )
            response.raise_for_status()
            path_query_rslt = response.json()
            base_paths[uuid] = path_query_rslt["path"]
        return base_paths

    def get_uuids(**kwargs):
        query = {
            "_source": [
                "entity_type", "creation_action", "dataset_type", "status", "uuid"
            ],
            "size": 5,  # Adjust if you have more than 10k datasets
            "query": {
                "bool": {
                    "must": [
                        {"match": {"creation_action": "Central Process"}},  # Processed datasets
                        {"match": {"entity_type": "Dataset"}},
                    ],
                    "should": [
                        {"match": {"status": "Published"}},  # Either Published or QA
                        {"match": {"status": "QA"}},
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        http_hook = HttpHook("POST", http_conn_id="search_api_connection")
        # TODO: find why the connection id isn't getting the v3 portion
        endpoint = f"/v3/portal/search"
        headers = {
            "Authorization": f"Bearer "
                             f"{get_auth_tok(**kwargs)}",
            "Content-Type": "application/json",
        }
        response = http_hook.run(endpoint=endpoint, headers=headers, json=query)
        response.raise_for_status()
        data = response.json()
        print(f"Long return {data}")

        df = pd.json_normalize(data, record_path=['hits', 'hits'])

        # Rename columns for clarity
        df = df.rename(columns={
            '_id': 'uuid',
            '_source.dataset_type': 'dataset_type',
        })

        # Keep only relevant columns
        columns_to_keep = ['uuid', 'dataset_type']
        df = df[[col for col in columns_to_keep if col in df.columns]]
        print(f"Found {len(df)}")

        # Get base paths for all datasets
        print("   Fetching dataset base paths from Ingest API...")
        dataset_ids = df['uuid'].tolist()
        base_paths = _get_dataset_base_paths(dataset_ids, get_auth_tok(**kwargs))

        # Add directory column to dataframe
        df['directory'] = df['uuid'].map(base_paths).fillna('')
        print(f"Retrieved {len(base_paths)} base paths")

        df.to_csv(Path(get_tmp_dir_path(kwargs["run_id"])) / "datasets.csv")

        return 0

    t_get_uuids = PythonOperator(
        task_id="get_uuids",
        python_callable=get_uuids,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    def __get_timestamp(line: str) -> datetime:
        timestamp_format = "%Y-%m-%d %H:%M:%S"
        timestamp_str = line[6:25]
        print(f"Timestamp: {timestamp_str}")
        return datetime.strptime(timestamp_str, timestamp_format)

    def __calculate_usage(starting_timestamp: datetime, ending_timestamp: datetime) -> timedelta:
        return ending_timestamp - starting_timestamp

    @task(task_id="calculate_statistics")
    def calculate_statistics(**kwargs):
        df = pd.read_csv(Path(get_tmp_dir_path(kwargs["run_id"]) / "datasets.csv"))
        startjob = r"\[step .+\] start$"
        endjob = r"\[step .+\] completed success$"
        gpu_task = r".*gpu.*"
        gpu = False
        starting_timestamp = None
        ending_timestamp = None
        df['cpu_usage'] = None
        df['gpu_usage'] = None
        cpu_usage = timedelta(seconds=0)
        gpu_usage = timedelta(seconds=0)
        for index, row in df.iterrows():
            path = Path(row.directory + "/session.log")
            try:
                with open(path, "r") as session_file:
                    for line in session_file:
                        if re.search(startjob, line):
                            starting_timestamp = __get_timestamp(line)
                            # Check if this is CPU or GPU and create a flag
                        if re.search(gpu_task, line):
                            gpu = True
                        if re.search(endjob, line) and starting_timestamp:
                            ending_timestamp = __get_timestamp(line)
                        if starting_timestamp and ending_timestamp:
                            # if CPU flag, append to CPU, else append to GPU
                            if gpu:
                                 gpu_usage += __calculate_usage(starting_timestamp,
                                                                ending_timestamp)
                            else:
                                cpu_usage += __calculate_usage(starting_timestamp, ending_timestamp)
                            starting_timestamp = None
                            ending_timestamp = None
                            gpu = False
            except FileNotFoundError:
                print(f"{path} not found")
            except PermissionError:
                print(f"{path} permission denied")
            except Exception as e:
                print(f"Error {e} in: {path}")
            finally:
                df.loc[index, "gpu_usage"] = gpu_usage
                df.loc[index, "cpu_usage"] = cpu_usage
                gpu_usage = timedelta(seconds=0)
                cpu_usage = timedelta(seconds=0)
                starting_timestamp = None
                ending_timestamp = None
        df.to_csv(Path(get_tmp_dir_path(kwargs["run_id"])) / "dataset_usage.csv")
        return 0

    t_calculate_statistics = calculate_statistics()

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")

    (
        t_create_tmpdir
        >> t_get_uuids
        >> t_calculate_statistics
        >> t_cleanup_tmpdir
    )
