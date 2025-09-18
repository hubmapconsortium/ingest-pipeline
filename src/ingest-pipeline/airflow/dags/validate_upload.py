from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from pprint import pprint

from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
)
from status_change.callbacks.failure_callback import FailureCallback
from status_change.status_manager import StatusChanger, Statuses
from utils import (
    HMDAG,
    get_auth_tok,
    get_preserve_scratch_resource,
    get_queue_resource,
    get_threads_resource,
    get_tmp_dir_path,
    pythonop_get_dataset_state,
)

from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

sys.path.append(airflow_conf.as_dict()["connections"]["SRC_PATH"].strip("'").strip('"'))

from submodules import ingest_validation_tools_upload  # noqa E402
from submodules import ingest_validation_tests, ingest_validation_tools_error_report

sys.path.pop()


# Following are defaults which can be overridden later on
default_args = {
    "owner": "hubmap",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["gesina@psc.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": FailureCallback(__name__),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "xcom_push": True,
    "queue": get_queue_resource("validate_upload"),
}

with HMDAG(
    "validate_upload",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("validate_upload"),
    },
) as dag:

    def find_uuid(**kwargs):
        uuid = kwargs["dag_run"].conf["uuid"]

        def my_callable(**kwargs):
            return uuid

        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        print("ds_rslt:")
        pprint(ds_rslt)

        # Push to xcom before checking ds_rslt to make UUID available to on_failure_callback
        lz_path = ds_rslt["local_directory_full_path"]
        uuid = ds_rslt["uuid"]  # 'uuid' may  actually be a DOI
        print(f"Finished uuid {uuid}")
        print(f"lz path: {lz_path}")
        kwargs["ti"].xcom_push(key="lz_path", value=lz_path)
        kwargs["ti"].xcom_push(key="uuid", value=uuid)

        for key in ["entity_type", "status", "uuid", "local_directory_full_path"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if ds_rslt["entity_type"] != "Upload":
            raise AirflowException(f"{uuid} is not an Upload")
        if ds_rslt["status"] not in ["New", "Submitted", "Invalid", "Processing"]:
            raise AirflowException(
                f"status of Upload {uuid} is not New, Submitted, Invalid, or Processing"
            )

    t_find_uuid = PythonOperator(
        task_id="find_uuid",
        python_callable=find_uuid,
        provide_context=True,
    )

    def run_validation(**kwargs):
        lz_path = kwargs["ti"].xcom_pull(key="lz_path")
        uuid = kwargs["ti"].xcom_pull(key="uuid")
        plugin_path = list(ingest_validation_tests.__path__)[0]

        ignore_globs = [uuid, "extras", "*metadata.tsv", "validation_report.txt"]
        app_context = {
            "entities_url": HttpHook.get_connection("entity_api_connection").host + "/entities/",
            "uuid_url": HttpHook.get_connection("uuid_api_connection").host + "/uuid/",
            "ingest_url": os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"],
            "request_header": {"X-Hubmap-Application": "ingest-pipeline"},
        }
        #
        # Uncomment offline=True below to avoid validating orcid_id URLs &etc
        #
        upload = ingest_validation_tools_upload.Upload(
            directory_path=Path(lz_path),
            dataset_ignore_globs=ignore_globs,
            upload_ignore_globs="*",
            plugin_directory=plugin_path,
            # offline=True,  # noqa E265
            add_notes=False,
            extra_parameters={
                "coreuse": get_threads_resource("validate_upload", "run_validation")
            },
            globus_token=get_auth_tok(**kwargs),
            app_context=app_context,
        )
        # Scan reports an error result
        report = ingest_validation_tools_error_report.ErrorReport(
            errors=upload.get_errors(plugin_kwargs=kwargs), info=upload.get_info()
        )
        validation_file_path = Path(get_tmp_dir_path(kwargs["run_id"])) / "validation_report.txt"
        with open(validation_file_path, "w") as f:
            f.write(report.as_text())
        kwargs["ti"].xcom_push(key="error_counts", value=report.counts)
        kwargs["ti"].xcom_push(key="validation_file_path", value=str(validation_file_path))

    t_run_validation = PythonOperator(
        task_id="run_validation",
        python_callable=run_validation,
        provide_context=True,
    )

    def send_status_msg(**kwargs):
        validation_file_path = Path(kwargs["ti"].xcom_pull(key="validation_file_path"))
        error_counts = kwargs["ti"].xcom_pull(key="error_counts")
        error_counts_print = (
            json.dumps(error_counts, indent=9).strip("{}").replace('"', "").replace(",", "")
        )
        error_counts_msg = "; ".join([f"{k}: {v}" for k, v in error_counts.items()])
        with open(validation_file_path) as f:
            report_txt = f.read()
        if report_txt.startswith("No errors!"):
            status = Statuses.UPLOAD_VALID
            extra_fields = {
                "validation_message": "",
            }
        else:
            status = Statuses.UPLOAD_INVALID
            extra_fields = {
                "validation_message": report_txt,
            }
            if not error_counts:
                logging.info("ERROR: status is invalid but error_counts not found.")
        logging.info(
            f"""
                     status: {status.value}
                     validation_message: {extra_fields['validation_message']}
                     """
        )
        if error_counts:
            logging.info(
                f"""
                ------------
                Error counts:
                {error_counts_print}
                ------------
                """
            )
        StatusChanger(
            kwargs["ti"].xcom_pull(key="uuid"),
            get_auth_tok(**kwargs),
            status=status,
            fields_to_overwrite=extra_fields,
            data_ingest_board_msg=error_counts_msg,
        ).update()

    t_send_status = PythonOperator(
        task_id="send_status",
        python_callable=send_status_msg,
        provide_context=True,
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")

    t_create_tmpdir >> t_find_uuid >> t_run_validation >> t_send_status >> t_cleanup_tmpdir
