import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import utils
from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
    SetDatasetProcessingOperator,
)
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


def get_dataset_uuid(**kwargs):
    lz_path, uuid = __get_lzpath_uuid(**kwargs)
    return uuid


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
    "queue": get_queue_resource("validate_dataset"),
    "on_failure_callback": utils.create_dataset_state_error_callback(get_dataset_uuid),
}


with HMDAG(
    "validate_dataset",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": utils.get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("validate_dataset"),
    },
) as dag:

    def __get_lzpath_uuid(**kwargs):
        if "lz_path" in kwargs["dag_run"].conf and "submission_id" in kwargs["dag_run"].conf:
            # These conditions are set by the hubap_api plugin when this DAG
            # is invoked from the ingest user interface
            lz_path = kwargs["dag_run"].conf["lz_path"]
            uuid = kwargs["dag_run"].conf["submission_id"]
        elif "parent_submission_id" in kwargs["dag_run"].conf:
            # These conditions are met when this DAG is triggered via
            # the launch_multi_analysis DAG.
            uuid_list = kwargs["dag_run"].conf["parent_submission_id"]
            assert len(uuid_list) == 1, f"{dag.dag_id} can only handle one uuid at a time"

            def my_callable(**kwargs):
                return uuid_list[0]

            ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
            if not ds_rslt:
                raise AirflowException(f"Invalid uuid/doi for group: {uuid_list}")
            if not "local_directory_full_path" in ds_rslt:
                raise AirflowException(f"Dataset status for {uuid_list[0]} has no full path")
            lz_path = ds_rslt["local_directory_full_path"]
            uuid = uuid_list[0]  # possibly translating a HuBMAP ID
        else:
            raise AirflowException("The dag_run does not contain enough information")
        return lz_path, uuid

    def run_validation(**kwargs):
        lz_path, uuid = __get_lzpath_uuid(**kwargs)
        plugin_path = [path for path in ingest_validation_tests.__path__][0]

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
                "coreuse": get_threads_resource("validate_dataset", "run_validation")
            },
            ignore_deprecation=True,
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
        op_kwargs={},
    )

    def send_status_msg(**kwargs):
        uuid = get_dataset_uuid(**kwargs)
        validation_file_path = Path(kwargs["ti"].xcom_pull(key="validation_file_path"))
        error_counts = kwargs["ti"].xcom_pull(key="error_counts")
        error_counts_print = (
            json.dumps(error_counts, indent=9).strip("{}").replace('"', "").replace(",", "")
        )
        error_counts_msg = "; ".join([f"{k}: {v}" for k, v in error_counts.items()])
        with open(validation_file_path) as f:
            report_txt = f.read()
        if report_txt.startswith("No errors!"):
            status = Statuses.DATASET_QA
            extra_fields = {
                "validation_message": "",
            }
        else:
            status = Statuses.DATASET_ERROR
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
            uuid,
            get_auth_tok(**kwargs),
            status=status,
            run_id=kwargs.get("run_id"),
            message=error_counts_msg,
        ).update()

    t_send_status = PythonOperator(
        task_id="send_status",
        python_callable=send_status_msg,
        provide_context=True,
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id="set_dataset_processing")

    (
        t_create_tmpdir
        >> t_set_dataset_processing
        >> t_run_validation
        >> t_send_status
        >> t_cleanup_tmpdir
    )
