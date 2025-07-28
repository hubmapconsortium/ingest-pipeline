import json
from multiprocessing import Pool
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

import pandas as pd

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
from airflow.configuration import conf as airflow_conf
from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

import utils

from utils import (
    assert_json_matches_schema,
    HMDAG,
    get_queue_resource,
    get_threads_resource,
    get_auth_tok,
    get_tmp_dir_path,
    get_preserve_scratch_resource,
)

import hashlib

# How many records to send to uuid-api in each block
RECS_PER_BLOCK = 100


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
    "queue": get_queue_resource("launch_checksums"),
}

with HMDAG(
    "launch_checksums",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": utils.get_tmp_dir_path,
        "src_path": (airflow_conf.as_dict()["connections"]["SRC_PATH"].strip('"').strip("'")),
        "THREADS": get_threads_resource("launch_checksums"),
        "preserve_scratch": get_preserve_scratch_resource("launch_checksums"),
    },
) as dag:

    def check_one_uuid(uuid: str, **kwargs):
        """
        Look up information on the given uuid or HuBMAP identifier.
        Returns:
        - the uuid, translated from an identifier if necessary
        - data type(s) of the dataset
        - local directory full path of the dataset
        """
        print(f"Starting uuid {uuid} check")
        my_callable = lambda **kwargs: uuid
        ds_rslt = utils.pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        print("ds_rslt:")
        pprint(ds_rslt)

        for key in ["status", "uuid", "local_directory_full_path", "metadata"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if not ds_rslt["status"] in ["Published"]:
            raise AirflowException(f"Dataset {uuid} is not Published.")

        return (
            ds_rslt["uuid"],
            ds_rslt["local_directory_full_path"],
        )

    def check_uuids(**kwargs):
        print("dag_run conf follows:")
        pprint(kwargs["dag_run"].conf)

        try:
            assert_json_matches_schema(
                kwargs["dag_run"].conf, "launch_checksums_metadata_schema.yml"
            )
        except AssertionError as e:
            print("invalid metadata follows:")
            pprint(kwargs["dag_run"].conf)
            raise

        uuid_l = kwargs["dag_run"].conf["uuid_list"]
        filtered_uuid_l = []
        filtered_path_l = []
        for uuid in uuid_l:
            (
                uuid,
                lz_path,
            ) = check_one_uuid(uuid, **kwargs)
            filtered_path_l.append(lz_path)
            filtered_uuid_l.append(uuid)

        print(f"filtered paths: {filtered_path_l}")
        print(f"filtered uuids: {filtered_uuid_l}")
        kwargs["ti"].xcom_push(key="lz_paths", value=filtered_path_l)
        kwargs["ti"].xcom_push(key="uuids", value=filtered_uuid_l)

    t_check_uuids = PythonOperator(
        task_id="check_uuids",
        python_callable=check_uuids,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": utils.encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    def calculate_checksums(file: Path) -> dict:
        """Calculate required checksums for a single file"""
        result = {}
        print(f"Processing file {file}")
        try:
            with open(str(file), "rb") as f:
                file_data = f.read()
                result["sha256_checksum"] = hashlib.sha256(file_data).hexdigest()
                result["md5_checksum"] = hashlib.md5(file_data).hexdigest()

        except Exception as e:
            result["error"] = str(e)

        return result

    def build_rec(parent_uuid, base_path) -> list:
        records = []
        for file in Path(base_path).glob("**/*"):
            if file.is_file():
                print("Reading " + str(file) + ".")
                record = {
                    "path": str(file),
                    "size": file.stat().st_size,
                    "parent_uuid": parent_uuid,
                    "base_path": base_path,
                }
                record.update(calculate_checksums(file))
                records.append(record)

        return records

    def build_checksum_tsv(
        **kwargs,
    ) -> None:
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        out_tsv = f"{tmpdir}/cksums.tsv"

        uuids = kwargs["ti"].xcom_pull("check_uuids", key="uuids")
        lz_paths = kwargs["ti"].xcom_pull("check_uuids", key="lz_paths")

        threads = get_threads_resource(dag.dag_id)

        with Pool(processes=threads) as pool:
            results = pool.starmap(build_rec, zip(uuids, lz_paths))

        flat_results = [result for result_list in results for result in result_list]

        output_df = pd.DataFrame(flat_results)
        output_df.to_csv(out_tsv, sep="\t", index=False)

    t_build_checksum_tsv = PythonOperator(
        task_id="build_checksum_tsv",
        python_callable=build_checksum_tsv,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": utils.encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    def send_block(block_df, **kwargs):
        headers = {
            "authorization": "Bearer " + get_auth_tok(**kwargs),
            "content-type": "application/json",
            "X-Hubmap-Application": "ingest-pipeline",
        }
        rec_l = []
        parent_uuid = block_df["parent_uuid"][0]
        parent_path = Path(block_df["base_path"][0])
        for idx, row in block_df.iterrows():  # pylint: disable=unused-variable
            this_path = Path(row["path"])
            rec_l.append(
                {
                    "path": str(this_path.relative_to(parent_path.parent)),
                    "sha256_checksum": row["sha256_checksum"],
                    "md5_checksum": row["md5_checksum"],
                    "size": row["size"],
                    "base_dir": "DATA_UPLOAD",
                }
            )
        data = {"entity_type": "FILE", "parent_ids": [parent_uuid], "file_info": rec_l}
        print("sending the following payload:")
        pprint(data)
        response = HttpHook("POST", http_conn_id="uuid_api_connection").run(
            endpoint=f"hmuuid?entity_count={len(rec_l)}",
            data=json.dumps(data),
            headers=headers,
            extra_options=[],
        )
        response.raise_for_status()
        print("response block follows")
        pprint(response.json())
        # TODO: We need to save the UUIDs elsewhere

    def send_checksums(**kwargs):
        run_id = kwargs["run_id"]
        tmp_dir_path = get_tmp_dir_path(run_id)

        uuids = kwargs["ti"].xcom_pull(task_ids="check_uuids", key="uuids")

        full_df = pd.read_csv(Path(tmp_dir_path) / "cksums.tsv", sep="\t")

        for uuid in uuids:
            uuid_df = full_df[full_df["parent_uuid"] == uuid]
            tot_recs = len(uuid_df)
            low_rec = 0
            while low_rec < tot_recs:
                block_df = full_df.iloc[low_rec : low_rec + RECS_PER_BLOCK]
                send_block(block_df, **kwargs)
                low_rec += RECS_PER_BLOCK

    t_send_checksums = PythonOperator(
        task_id="send_checksums",
        python_callable=send_checksums,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": utils.encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    # TODO: Generate a SQL file to insert data into DRS

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")

    (
        t_create_tmpdir
        >> t_check_uuids
        >> t_build_checksum_tsv
        >> t_send_checksums
        >> t_cleanup_tmpdir
    )
