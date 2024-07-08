import os
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta
import logging

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

import utils
from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    HMDAG,
    get_queue_resource,
    )

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'xcom_push': True,
    'queue': get_queue_resource('validation_test'),
}

with HMDAG('standardize_extensions',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           ) as dag:

    app_client_secret = airflow_conf.as_dict().get('connections', {}).get('APP_CLIENT_SECRET')
    assert app_client_secret is str
    op_kwargs = {'crypt_auth_tok': utils.encrypt_tok(app_client_secret).decode()}

    def find_uuid(**kwargs):
        try:
            assert_json_matches_schema(kwargs["dag_run"].conf,
                                       "standardize_extension_schema.yml")
        except AssertionError:
            print("invalid metadata follows:")
            pprint(kwargs["dag_run"].conf)
            raise

        uuid = kwargs["dag_run"].conf["uuid"]

        ds_rslt = utils.pythonop_get_dataset_state(
            dataset_uuid_callable=lambda **kwargs: uuid,
            **kwargs
        )
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")

        for key in ["entity_type", "status", "uuid", "local_directory_full_path"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if not ds_rslt["entity_type"] in ["Upload"]:
            raise AirflowException(f"Entity {uuid} is not an Upload")

        if not ds_rslt["status"] in ["New", "Invalid", "Error"]:
            raise AirflowException(f"Dataset {uuid} status must be 'New', 'Invalid', or 'Error'; current status is {ds_rslt['status']}")

        local_dirs = {upload["uuid"]: upload["local_directory_full_path"] for upload in ds_rslt}
        return local_dirs  # causing it to be put into xcom

    t_find_uuid = PythonOperator(
        task_id='find_uuid',
        python_callable=find_uuid,
        provide_context=True,
        op_kwargs=op_kwargs
        )

    def check_directories(**kwargs):
        local_dirs = kwargs['ti'].xcom_pull(task_ids="find_uuid").copy()
        dir_list = []
        uuids = []
        for uuid, dir in local_dirs.items():
            # TODO
            assert Path(dir).exists(), f"Local directory path {dir} for uuid {uuid} does not exist!"
            assert Path(dir).parts[-1] == uuid, f"Upload directory {dir} part {Path(dir).parts[-1]} and UUID {uuid} do not match; double check."
            dir_list.append(dir)
            uuids.append(uuid)
        kwargs["ti"].xcom_push(key="uuids", value=uuids)
        return dir_list

    t_check_directories = PythonOperator(
        task_id="check_directories",
        python_callable=check_directories,
        provide_context=True,
        op_kwargs=op_kwargs
        )

    def find_target_files(**kwargs):
        """
        Current use case is just for tif/tiff conversion;
        DAG is written more generally but given a tif/tiff default.
        Can also pass {"find_only": True} to find and list but not replace
        instances of the "target" value; default is to find and replace.
        """
        extension_pair = kwargs.get("extension_pair", {"target": "tif", "replacement": "tiff"})
        for extension_action, ext in extension_pair.items():
            if not ext.startswith("."):
                extension_pair[extension_action] = f".{ext}"
        target = extension_pair.get("target")
        replacement = extension_pair.get("replacement")
        assert target and replacement, f"Missing either target or replacement in extension_pair kwargs. Value passed in: {extension_pair}"
        # Crawl through dir for a given uuid and locate all instances of target
        target_filepaths = []
        directories = kwargs['ti'].xcom_pull(task_ids="check_directories")
        for root_path in directories:
            for dirpath, _, filenames in os.walk(root_path):
                target_filepaths.extend([os.path.join(dirpath, file) for file in filenames if file.endswith(target)])
        if kwargs.get("verbose", True):
            logging.info(f"Files matching extension {target}:")
            logging.info(target_filepaths)
        kwargs["ti"].xcom_push(key="target", value=target)
        kwargs["ti"].xcom_push(key="replacement", value=replacement)
        kwargs["ti"].xcom_push(key="target_filepaths", value=target_filepaths)

    t_find_target_files = PythonOperator(
        task_id='find_target_files',
        python_callable=find_target_files,
        provide_context=True,
        op_kwargs=op_kwargs
        )

    def standardize_extensions(**kwargs):
        find_only = kwargs.get("find_only", False)
        target_filepaths = kwargs['ti'].xcom_pull(task_ids="find_target_files", key="target_filepaths")
        target = kwargs['ti'].xcom_pull(task_ids="find_target_files", key="target")
        replacement = kwargs['ti'].xcom_pull(task_ids="find_target_files", key="replacement")
        for file in target_filepaths:
            filename, ext = os.path.splitext(file)
            assert ext == target, f"File path {file} is not the correct target extension, something went wrong in find_target_files step; exiting without further changes."
            if find_only:
                logging.info(f"Would have changed {file} to {filename + replacement}")
                continue
            logging.info(f"Renaming {file} to {filename + replacement}.")
            os.rename(file, filename + replacement)
        uuids = kwargs['ti'].xcom_pull(task_ids="check_directories", key="uuids")
        logging.info(f"Standardize extensions complete for UUIDs {uuids}!")


    t_standardize_extensions = PythonOperator(
        task_id='standardize_extensions',
        python_callable=standardize_extensions,
        provide_context=True,
        op_kwargs=op_kwargs
        )

    t_find_uuid >> t_check_directories >> t_find_target_files >> t_standardize_extensions
