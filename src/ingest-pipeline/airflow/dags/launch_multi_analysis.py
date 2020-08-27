import sys
import os
import json
import shlex
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.http_hook import HttpHook
from airflow.configuration import conf as airflow_conf

import utils

from utils import localized_assert_json_matches_schema as assert_json_matches_schema


def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    return None


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
    'queue': utils.map_queue_name('general'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error)
}


def uuid_fun(**kwargs):
    return kwargs['uuid']


with DAG('launch_multi_analysis', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         max_active_runs=1,
         user_defined_macros={'tmp_dir_path' : utils.get_tmp_dir_path}
         ) as dag:

    def check_uuids(**kwargs):
        utils.pythonop_trigger_target(**kwargs)
        print('dag_run conf follows:')
        pprint(kwargs['dag_run'].conf)
        uuid_l = ['HBM959.DHDL.956','3e337deec405743f52347a8d526931f0', 'foo'] # get from conf
        for uuid in uuid_l:
            print(f'Starting uuid {uuid}')
            my_callable = lambda **kwargs: uuid
            print('my callable returns: ' + my_callable())
            rslt=utils.pythonop_get_dataset_state(dataset_uuid_callable=my_callable,
                                                  http_conn_id='ingest_api_connection',
                                                  **kwargs)
            print(f'rslt: {rslt}   ')
            print(f'Finished uuid {uuid}')

    check_uuids_t = PythonOperator(
        task_id='check_uuids',
        python_callable=check_uuids,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                 ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )

    dag >> check_uuids_t

