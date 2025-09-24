import pytz
from airflow.api.common.trigger_dag import trigger_dag

import utils
import os
import yaml
import time
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta
import pandas as pd

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from requests.exceptions import HTTPError
from airflow.decorators import task
from airflow.models.dagrun import DagRun


from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

from utils import (
    pythonop_maybe_keep,
    make_send_status_msg_function,
    get_tmp_dir_path,
    get_auth_tok,
    pythonop_get_dataset_state,
    pythonop_set_dataset_state,
    find_matching_endpoint,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_soft_data_assaytype,
    _get_scratch_base_path,
)

from misc.tools.split_and_create import reorganize
from misc.tools.set_standard_protections import process_one_uuid
from misc.tools.survey import EntityFactory


# Following are defaults which can be overridden later on
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
    "queue": get_queue_resource("undo_upload_reorganization"),
}


with HMDAG(
    "undo_upload_reorganization",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("undo_upload_reorganization"),
    },
) as dag:
    # Failure cases
    # - Upload failed during first step
    #   - This would mean that it did not create all of its children datasets.
    #   - We don't have to worry about moving files around, since that's done in the second step.
    #   - We just need to delete the children datasets, which is something that Pitt will do
    # - Upload failed during second step
    #   - Upload failed during data movement
    #       - We definitely need to move data back. In this case, there should be a file somewhere that has the full list of dataset UUIDs
    #       - We iterate over the dataset UUIDs, move data back to the upload under the appropriate data directory (what is in the original metadata)
    #       - Expect that we receive a run id (e5ce5afb6653e5d88d1ba2fe1cb89aa9_reorganize.upload_2025-09-11T10:47:58.855034-04:00) as part of the config.
    #       - Using the run id, we can read in the frozen file, and use the "new_uuid" field to move data back
    #   - Upload failed during Upload status update
    #       - Same steps as above
    #   - Upload failed during dataset updates
    #       - If we want to move data back we can, but it would probably be easiest to just bulk update/re-ingest metadata
    #   - Upload failed during re-indexing
    #       - Not really anything to fix here, just need to reindex things

    @task()
    def instantiate_factories(**kwargs):
        # Need an EntityFactory
        return EntityFactory(
            get_auth_tok(**kwargs),
            instance=find_matching_endpoint(HttpHook.get_connection("entity_api_connection").host),
        )

    @task()
    def check_conf(ef: EntityFactory, dag_run: DagRun):
        if "prev_run_id" not in dag_run.conf or "upload_uuid" not in dag_run.conf:
            print("Missing prev_run_id or upload_uuid in configuration.")
            raise AirflowException("Missing required configuration: prev_run_id or upload_uuid")

        upload = ef.get(dag_run.conf["upload_uuid"])
        if upload.prop_dct["entity_type"] != "Upload":
            print(
                f"UUID {dag_run.conf['upload_uuid']} is not an Upload. Entity type is {upload.prop_dct['entity_type']}."
            )
            raise AirflowException(f"UUID {dag_run.conf['upload_uuid']} is not an Upload")

        if not (_get_scratch_base_path() / dag_run.conf["prev_run_id"]).exists():
            print(f"Could not find path {_get_scratch_base_path() / dag_run.conf['prev_run_id']}")
            raise AirflowException(
                f"Could not find path {_get_scratch_base_path() / dag_run.conf['prev_run_id']}"
            )

        dryrun = dag_run.conf.get("dryrun", False)
        if dryrun:
            print("DRYRUN MODE: Will only print paths, no actual data movement")

        print("Configuration validation passed")
        return True

    @task()
    def move_data(ef: EntityFactory, dag_run: DagRun):
        import shutil

        upload_uuid = dag_run.conf["upload_uuid"]
        prev_run_id = dag_run.conf["prev_run_id"]
        dryrun = dag_run.conf.get("dryrun", False)

        # Get upload path
        upload = ef.get(upload_uuid)
        upload_path = upload.full_path

        # Read frozen TSV to get original data_path mapping
        run_tmp_path = _get_scratch_base_path() / prev_run_id
        uuid_to_data_path = {}

        for tsv in run_tmp_path.glob("frozen_source_df*.tsv"):
            tmp_df = pd.read_csv(tsv, separator="\t")
            for _, row in tmp_df.iterrows():
                uuid_to_data_path[row["new_uuid"]] = row["data_path"]

        moved_count = 0
        for uuid, original_data_path in uuid_to_data_path.items():
            # Get dataset's current path
            dataset_path = ef.get_full_path(uuid)

            target_path = upload_path / original_data_path

            action_prefix = "DRYRUN: Would move" if dryrun else "Moving"
            print(f"{action_prefix} data from {dataset_path} back to {target_path}")

            # Create parent directory if it doesn't exist
            if dryrun:
                print(f"DRYRUN: Would create parent directory {target_path.parent}")
            else:
                target_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                # Move all contents from dataset back to original location
                if dataset_path.exists():
                    items_to_move = list(dataset_path.glob("*"))
                    if not items_to_move:
                        print(f"Warning: Dataset path {dataset_path} exists but is empty")

                    for item in items_to_move:
                        dest_item = target_path / item.name

                        if item.is_dir():
                            if dest_item.exists():
                                if dryrun:
                                    print(f"DRYRUN: Would remove existing directory {dest_item}")
                                else:
                                    shutil.rmtree(dest_item)

                            if dryrun:
                                print(f"DRYRUN: Would move directory {item} -> {dest_item}")
                            else:
                                shutil.move(str(item), str(dest_item))
                        else:
                            if dest_item.exists():
                                if dryrun:
                                    print(f"DRYRUN: Would remove existing file {dest_item}")
                                else:
                                    dest_item.unlink()

                            if dryrun:
                                print(f"DRYRUN: Would move file {item} -> {dest_item}")
                            else:
                                shutil.move(str(item), str(dest_item))

                    moved_count += 1
                else:
                    warning_prefix = "DRYRUN: " if dryrun else ""
                    print(f"{warning_prefix}Warning: Dataset path {dataset_path} does not exist")
            except Exception as e:
                error_prefix = "DRYRUN: " if dryrun else ""
                print(f"{error_prefix}Error moving data for UUID {uuid}: {e}")

        mode_str = "DRYRUN: Would have moved" if dryrun else "Successfully moved"
        print(f"{mode_str} data for {moved_count} out of {len(uuids)} datasets")
        return moved_count > 0

    # Task definitions
    entity_factory_task = instantiate_factories()
    config_validation_task = check_conf(entity_factory_task)
    move_data_task = move_data(entity_factory_task)

    # Task dependencies
    entity_factory_task >> config_validation_task >> move_data_task
