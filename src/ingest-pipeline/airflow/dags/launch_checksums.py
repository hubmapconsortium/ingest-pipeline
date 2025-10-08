import json
from multiprocessing import Pool
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta
from requests.exceptions import HTTPError

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

    def send_block(parent_uuid, block_df, **kwargs):
        headers = {
            "authorization": "Bearer " + get_auth_tok(**kwargs),
            "content-type": "application/json",
            "X-Hubmap-Application": "ingest-pipeline",
        }
        rec_l = []
        for idx, row in block_df.iterrows():  # pylint: disable=unused-variable
            parent_path = Path(row["base_path"])
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
        rec_df = pd.DataFrame(rec_l)
        response_df = pd.DataFrame(response.json())
        print(response_df)
        return pd.merge(rec_df, response_df, left_on="path", right_on="file_path")

    def send_checksums(**kwargs):
        run_id = kwargs["run_id"]
        tmp_dir_path = get_tmp_dir_path(run_id)

        uuids = kwargs["ti"].xcom_pull(task_ids="check_uuids", key="uuids")
        dryrun = kwargs["dag_run"].conf.get("dryrun", False)

        full_df = pd.read_csv(Path(tmp_dir_path) / "cksums.tsv", sep="\t")

        uuid_responses = pd.DataFrame()
        for uuid in uuids:
            uuid_df = full_df[full_df["parent_uuid"] == uuid]
            tot_recs = len(uuid_df)
            low_rec = 0

            while low_rec < tot_recs:
                block_df = uuid_df.iloc[low_rec : low_rec + RECS_PER_BLOCK]

                if not dryrun:
                    uuid_responses = pd.concat(
                        [uuid_responses, send_block(uuid, block_df, **kwargs)]
                    )

                low_rec += RECS_PER_BLOCK

        # With the responses from the uuid API, we need to add the file uuids to our TSV.
        # Those uuids will be used by the DRS.
        if not dryrun:
            full_df = full_df.merge(
                uuid_responses[["sha256_checksum", "hm_uuid"]], on="sha256_checksum"
            )

        full_df.to_csv(Path(tmp_dir_path) / "cksums_and_uuids.tsv", sep="\t")

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

    def generate_drs_entries(**kwargs):
        run_id = kwargs["run_id"]
        tmp_dir_path = get_tmp_dir_path(run_id)

        # Let's generate TSVs that will be inserted into the DRS
        # manifest.tsv
        # manifest_id, uuid, hubmap_id, creation_date, dataset_type, directory, doi_url, group_name, is_protected,
        # number_of_files, pretty_size

        uuids = kwargs["ti"].xcom_pull(task_ids="check_uuids", key="uuids")
        lz_paths = kwargs["ti"].xcom_pull(task_ids="check_uuids", key="lz_paths")
        full_df = pd.read_csv(Path(tmp_dir_path) / "cksums_and_uuids.tsv", sep="\t")
        manifest_df = []

        # Hit entity API
        headers = {
            "authorization": f"Bearer {get_auth_tok(**kwargs)}",
            "content-type": "application/json",
            "Cache-Control": "no-cache",
            "X-Hubmap-Application": "ingest-pipeline",
        }
        http_hook = HttpHook("GET", http_conn_id="entity_api_connection")

        for uuid, lz_path in zip(uuids, lz_paths):
            endpoint = f"entities/{uuid}?exclude=direct_ancestors.files"

            try:
                response = http_hook.run(
                    endpoint, headers=headers, extra_options={"check_response": False}
                )
                response.raise_for_status()
                ds_rslt = response.json()
                files_for_uuid = full_df[full_df["parent_uuid"] == uuid]
                num_files = len(files_for_uuid)
                size_files = files_for_uuid["size"].sum()
                # We need to pull out the hubmap_id, creation_date, dataset_type, doi_url, group_name
                # We need to calc whether its protected or not (can tell based on filepath)
                # We need to calc number of files and the pretty size. We should be able to gather this from
                # The full_df by filtering on the hubmap id. len() of that block will get us the number of files
                # Summing the size column of those rows and then applying some formatting will give us the pretty_size
                manifest_df.append(
                    {
                        "uuid": uuid,
                        "hubmap_id": ds_rslt["hubmap_id"],
                        "creation_date": datetime.fromtimestamp(
                            ds_rslt["created_timestamp"] / 1000
                        ),
                        "dataset_type": ds_rslt["dataset_type"],
                        "directory": lz_path,
                        "doi_url": ds_rslt["doi_url"],
                        "group_name": ds_rslt["group_name"],
                        "is_protected": 0 if "public" in lz_path else 1,
                        "number_of_files": num_files,
                        "pretty_size": size_files,
                    }
                )

            except HTTPError as e:
                print(f"UUID {uuid} had error: {e}")

        manifest_df = pd.DataFrame(manifest_df)

        # files.tsv
        # hubmap_id, drs_uri (unused), name, dbgap_study_id, file_uuid, checksum, size
        # We need to look up the hubmap id from the manifest_df, everything else should already be in the full_df
        full_df = full_df.merge(
            manifest_df[["uuid", "hubmap_id"]], left_on="parent_uuid", right_on="uuid"
        )
        full_df.rename(
            columns={
                "path": "name",
                "hm_uuid": "file_uuid",
                "sha256_checksum": "checksum",
            },
            inplace=True,
        )
        full_df = full_df.drop(
            columns=["parent_uuid", "base_path", "md5_checksum", "uuid", "Unnamed: 0"]
        )

        manifest_df.to_csv(Path(tmp_dir_path) / "manifest.tsv", sep="\t")
        full_df.to_csv(Path(tmp_dir_path) / "files.tsv", sep="\t")

    t_generate_drs_entries = PythonOperator(
        task_id="generate_drs_entries",
        python_callable=generate_drs_entries,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": utils.encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir")

    (
        t_create_tmpdir
        >> t_check_uuids
        >> t_build_checksum_tsv
        >> t_send_checksums
        >> t_generate_drs_entries
        >> t_cleanup_tmpdir
    )
