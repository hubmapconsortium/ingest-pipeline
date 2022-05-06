import sys
import os
import json
import shlex
import ast
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.hooks.http_hook import HttpHook
from airflow.exceptions import AirflowException
from airflow.configuration import conf as airflow_conf
from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

import utils

from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    HMDAG,
    get_queue_resource,
    get_threads_resource,
)

def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    return None  # We better not have such an error, because there is no place to put it


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
    'queue': get_queue_resource('launch_checksums'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error)
}


with HMDAG('launch_checksums', 
           schedule_interval=None, 
           is_paused_upon_creation=False, 
           default_args=default_args,
           user_defined_macros={'tmp_dir_path' : utils.get_tmp_dir_path,
                                'src_path' : (airflow_conf.as_dict()['connections']['SRC_PATH']
                                              .strip('"').strip("'")),
                                'THREADS' : get_threads_resource('launch_checksums')
                            }
       ) as dag:

    def check_uuid(**kwargs):
        print('dag_run conf follows:')
        pprint(kwargs['dag_run'].conf)

        try:
            assert_json_matches_schema(kwargs['dag_run'].conf,
                                       'launch_checksums_metadata_schema.yml')
        except AssertionError as e:
            print('invalid metadata follows:')
            pprint(kwargs['dag_run'].conf)
            raise
        
        uuid = kwargs['dag_run'].conf['uuid']
        filtered_uuid = None
        filtered_path = None
        filtered_data_type = None
        print(f'Starting uuid {uuid}')
        my_callable = lambda **kwargs: uuid
        rslt=utils.pythonop_get_dataset_state(
            dataset_uuid_callable=my_callable,
            **kwargs
        )
        if not rslt:
            raise AirflowException(f'Invalid uuid/doi: {uuid}')
        print('rslt:')
        pprint(rslt)
        assert 'dataset' in rslt, f"Status for {uuid} has no dataset entry"
        ds_rslt = rslt['dataset']

        for key in ['status', 'uuid', 'data_types', 'local_directory_full_path']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

#         if not ds_rslt['status'] in ['Published']:
#             raise AirflowException(f'Dataset {uuid} is not QA or better')

        dt = ds_rslt['data_types']
        if isinstance(dt, str) and dt.startswith('[') and dt.endswith(']'):
            dt = ast.literal_eval(dt)
        print(f'parsed dt: {dt}')
        if isinstance(dt, list):
            if dt:
                if len(dt) == 1:
                    filtered_data_type = dt[0]
                else:
                    filtered_data_type = tuple(dt)
            else:
                raise AirflowException(f'Dataset data_types for {uuid} is empty')
        else:
            filtered_data_type = dt

        lz_path = ds_rslt['local_directory_full_path']
        filtered_path = lz_path
        filtered_uuid = ds_rslt['uuid']
        print(f'Finished uuid {filtered_uuid}')
        print(f'filtered data types: {filtered_data_type}')
        print(f'filtered path: {filtered_path}')
        kwargs['ti'].xcom_push(key='assay_type', value=filtered_data_type)
        kwargs['ti'].xcom_push(key='lz_path', value=filtered_path)
        kwargs['ti'].xcom_push(key='uuid', value=filtered_uuid)

    check_uuid_t = PythonOperator(
        task_id='check_uuid',
        python_callable=check_uuid,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                 ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )


    t_gen_cksum_table = BashOperator(
        task_id='gen_cksum_table',
        bash_command="""
        tmp_dir="{{tmp_dir_path(run_id)}}" ; \
        ds_dir="{{ti.xcom_pull(task_ids='check_uuid', key='lz_path')}}" ; \
        src_path="{{src_path}}" ; \
        python "$src_path"/misc/tools/checksum_md5.py \
            -i "${ds_dir}" -o "${tmp_dir}"/cksums.tsv \
            -n "{{THREADS}}"
        """
        )

    
    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
#    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')
    t_cleanup_tmpdir = DummyOperator(task_id='cleanup_tmpdir')

    (dag >> t_create_tmpdir
     >> check_uuid_t
     >> t_gen_cksum_table
     >> t_cleanup_tmpdir
     )

