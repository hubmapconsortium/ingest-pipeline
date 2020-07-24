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

def get_dataset_uuid(**kwargs):
    #return 'b40eb3abccf2341f274cfd4ba809c03e'
    return '48c8dd2ad06aa23e36c095c9088a4913'


def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    rslt = get_dataset_uuid(**kwargs)
    return rslt


default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    'xcom_push': True,
    'queue': utils.map_queue_name('general'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error)
}


with DAG('reset_submission_to_new', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         max_active_runs=1,
         user_defined_macros={'tmp_dir_path' : utils.get_tmp_dir_path}
         ) as dag:

    t_set_dataset_new = PythonOperator(
        task_id='set_dataset_new',
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        op_kwargs = {'dataset_uuid_callable' : get_dataset_uuid,
                     'http_conn_id' : 'ingest_api_connection',
                     'endpoint' : '/datasets/status',
                     'ds_state' : 'New',
                     'message' : 'Resetting state to NEW',
                     'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                          ['connections']['APP_CLIENT_SECRET']).decode()
                     }
    )


    dag >> t_set_dataset_new


