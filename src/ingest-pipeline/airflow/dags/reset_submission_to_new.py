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

UUIDS_TO_RESET = [
    #'2c467ffa1d01c41effb7057d7d329c8f',
    #'48c8dd2ad06aa23e36c095c9088a4913',
    #'08ee9f5575339641eb9f8fb17cc1d1bd'
    #"0eb5e457b4855ce28531bc97147196b6",  # HCA scRNAseq-10x-v2
    #"33874bc3c95b6b41cbc387d2826d88eb",  # San Diego scRNAseq-SNARE
    #"4758c75194e26ef598ec611916042f51",  # sample Upload on DEV
    #"060dfa0fdf2b840864f62d2cd1a7a456",  # GE CellDIVE
    #"488f364142c308a9692e0b529f6697dd",  # GUDMAP scRNAseq disguised as UCSD scRNAseq
    '2c467ffa1d01c41effb7057d7d329c8f',
    '48c8dd2ad06aa23e36c095c9088a4913',
    '08ee9f5575339641eb9f8fb17cc1d1bd'

    ]


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


with DAG('reset_submission_to_new', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         max_active_runs=1,
         user_defined_macros={'tmp_dir_path' : utils.get_tmp_dir_path}
         ) as dag:

    prev = dag
    
    for idx, uuid in enumerate(UUIDS_TO_RESET):

        this_t = PythonOperator(
            task_id=f'set_dataset_new_{idx}',
            python_callable=utils.pythonop_set_dataset_state,
            provide_context=True,
            op_kwargs = {'dataset_uuid_callable' : uuid_fun,
                         'ds_state' : 'New',
                         'message' : 'Resetting state to NEW',
                         'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                              ['connections']['APP_CLIENT_SECRET']).decode(),
                         'uuid': uuid
                         }
        )
        
        prev >> this_t
        prev = this_t


