import sys
import os
import re
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

sys.path.append(airflow_conf.as_dict()['connections']['SRC_PATH']
                .strip("'").strip('"'))
from submodules import (ingest_validation_tools_upload,  # noqa E402
                        ingest_validation_tools_error_report,
                        ingest_validation_tests)
sys.path.pop()

FIND_SCRATCH_REGEX = r'/.+/scratch/[^/]+/'

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

with HMDAG('diagnose_failure',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           ) as dag:

    def find_uuid(**kwargs):
        try:
            assert_json_matches_schema(kwargs['dag_run'].conf,
                                       'diagnose_failure_schema.yml')
        except AssertionError:
            print('invalid metadata follows:')
            pprint(kwargs['dag_run'].conf)
            raise

        uuid = kwargs['dag_run'].conf['uuid']

        def my_callable(**kwargs):
            return uuid

        ds_rslt = utils.pythonop_get_dataset_state(
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

        if not ds_rslt['entity_type'] in ['Dataset']:
            raise AirflowException(f'Entity {uuid} is not a Dataset')

        if not ds_rslt['status'] in ['Error']:
            raise AirflowException(f'Dataset {uuid} is not in Error state')

        lz_path = ds_rslt['local_directory_full_path']
        uuid = ds_rslt['uuid']  # original 'uuid' may  actually be a DOI
        print(f'Finished uuid {uuid}')
        print(f'lz path: {lz_path}')
        return ds_rslt  # causing it to be put into xcom

    t_find_uuid = PythonOperator(
        task_id='find_uuid',
        python_callable=find_uuid,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': (
                utils.encrypt_tok(airflow_conf.as_dict()
                                  ['connections']['APP_CLIENT_SECRET'])
                .decode()
                ),
            }
        )

    def find_scratch(**kwargs):
        info_dict = kwargs['ti'].xcom_pull(task_ids="find_uuid").copy()
        dir_path = Path(info_dict['local_directory_full_path'])
        session_log_path = dir_path / 'session.log'
        assert session_log_path.exists(), 'session.log is not in the dataset directory'
        regex = re.compile(FIND_SCRATCH_REGEX)
        scratch_path = None
        for line in open(session_log_path):
            match = regex.search(line)
            if match:
                scratch_path = match.group(0)
                break
        if scratch_path:
            info_dict['scratch_path'] = scratch_path
        return info_dict  # causing it to be put into xcom

    t_find_scratch = PythonOperator(
        task_id='find_scratch',
        python_callable=find_scratch,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': (
                utils.encrypt_tok(airflow_conf.as_dict()
                                  ['connections']['APP_CLIENT_SECRET'])
                .decode()
            ),
        }
    )

    def run_diagnostics(**kwargs):
        info_dict = kwargs['ti'].xcom_pull(task_ids="find_scratch").copy()
        for key in info_dict:
            logging.info(f'{key.upper()}: {info_dict[key]}')
        return info_dict  # causing it to be put into xcom

    t_run_diagnostics = PythonOperator(
        task_id='run_diagnostics',
        python_callable=run_diagnostics,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': (
                utils.encrypt_tok(airflow_conf.as_dict()
                                  ['connections']['APP_CLIENT_SECRET'])
                .decode()
            ),
        }
    )

    t_find_uuid >> t_find_scratch >> t_run_diagnostics
