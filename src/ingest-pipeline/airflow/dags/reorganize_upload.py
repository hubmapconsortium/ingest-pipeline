import sys
import os
import ast
import json
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf as airflow_conf
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook

import utils
from utils import (
    get_tmp_dir_path, get_auth_tok,
    map_queue_name, pythonop_get_dataset_state,
    localized_assert_json_matches_schema as assert_json_matches_schema
    )

sys.path.append(airflow_conf.as_dict()['connections']['SRC_PATH']
                .strip("'").strip('"'))
from submodules import (ingest_validation_tools_upload,  # noqa E402
                        ingest_validation_tools_error_report,
                        ingest_validation_tests)
sys.path.pop()

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
    'queue': map_queue_name('general')
}


with DAG('reorganize_upload',
         schedule_interval=None,
         is_paused_upon_creation=False,
         user_defined_macros={'tmp_dir_path' : get_tmp_dir_path},
         default_args=default_args,
         ) as dag:

    def find_uuid(**kwargs):
        uuid = kwargs['dag_run'].conf['uuid']

        def my_callable(**kwargs):
            return uuid

        ds_rslt = pythonop_get_dataset_state(
            dataset_uuid_callable=my_callable,
            **kwargs
        )
        if not ds_rslt:
            raise AirflowException(f'Invalid uuid/doi for group: {uuid}')
        print('ds_rslt:')
        pprint(ds_rslt)

        for key in ['entity_type', 'status', 'uuid', 'data_types',
                    'local_directory_full_path']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if ds_rslt['entity_type'] != 'Upload':
            raise AirflowException(f'{uuid} is not an Upload')
        if ds_rslt['status'] not in ['New', 'Submitted', 'Invalid', 'Processing']:
            raise AirflowException(f'status of Upload {uuid} is not New, Submitted, Invalid, or Processing')

        lz_path = ds_rslt['local_directory_full_path']
        uuid = ds_rslt['uuid']  # 'uuid' may  actually be a DOI
        print(f'Finished uuid {uuid}')
        print(f'lz path: {lz_path}')
        kwargs['ti'].xcom_push(key='lz_path', value=lz_path)
        kwargs['ti'].xcom_push(key='uuid', value=uuid)

    t_find_uuid = PythonOperator(
        task_id='find_uuid',
        python_callable=find_uuid,
        provide_context=True,
        op_kwargs={
            }
        )

    t_create_tmpdir = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir {{tmp_dir_path(run_id)}}'
        )

    t_cleanup_tmpdir = BashOperator(
        task_id='cleanup_temp_dir',
        bash_command='rm -r {{tmp_dir_path(run_id)}}',
        trigger_rule='all_success'
        )

    (dag >> t_create_tmpdir >> t_find_uuid >> t_cleanup_tmpdir)
