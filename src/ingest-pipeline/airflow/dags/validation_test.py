import sys
import os
import yaml
import json
import ast
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf as airflow_conf
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.operators.multi_dagrun import TriggerMultiDagRunOperator
from airflow.hooks.http_hook import HttpHook

from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator
import utils

from utils import localized_assert_json_matches_schema as assert_json_matches_schema


def get_src_path(**kwargs):
    rslt = airflow_conf.as_dict()['connections']['SRC_PATH']
    return rslt.strip("'").strip('"')
    

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
    'queue': utils.map_queue_name('general')
}


with DAG('validation_test', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         user_defined_macros={'get_src_path' : get_src_path}         
         ) as dag:

    def find_uuid(**kwargs):
        try:
            assert_json_matches_schema(kwargs['dag_run'].conf,
                                       'validation_test_schema.yml')
        except AssertionError as e:
            print('invalid metadata follows:')
            pprint(kwargs['dag_run'].conf)
            raise
        
        uuid = kwargs['dag_run'].conf['uuid']
        my_callable = lambda **kwargs: uuid
        rslt=utils.pythonop_get_dataset_state(dataset_uuid_callable=my_callable,
                                              http_conn_id='ingest_api_connection',
                                              **kwargs)
        if not rslt:
            raise AirflowException(f'Invalid uuid/doi for group: {uuid}')
        print('rslt:')
        pprint(rslt)
        assert 'dataset' in rslt, f"Status for {uuid} has no dataset entry"
        ds_rslt = rslt['dataset']

        for key in ['status', 'uuid', 'data_types', 'local_directory_full_path']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if not ds_rslt['status'] in ['New', 'Invalid']:
            raise AirflowException(f'Dataset {uuid} is not New or Invalid')

        dt = ds_rslt['data_types']
        if isinstance(dt, str) and dt.startswith('[') and dt.endswith(']'):
            dt = ast.literal_eval(dt)
        print(f'parsed dt: {dt}')
        if isinstance(dt, list):
            if dt:
                if len(dt) == 1:
                    filtered_data_types = [dt[0]]
                else:
                    filtered_data_types = [tuple(dt)]
            else:
                raise AirflowException(f'Dataset data_types for {uuid} is empty')
        else:
            filtered_data_types = [dt]

        lz_path = ds_rslt['local_directory_full_path']
        uuid = ds_rslt['uuid']  # in case the original 'uuid' was actually a DOI
        print(f'Finished uuid {uuid}')
        print(f'filtered data types: {filtered_data_types}')
        print(f'lz path: {lz_path}')
        kwargs['ti'].xcom_push(key='assay_type', value=filtered_data_types)
        kwargs['ti'].xcom_push(key='lz_path', value=lz_path)
        kwargs['ti'].xcom_push(key='uuid', value=uuid)

    t_find_uuid = PythonOperator(
        task_id='find_uuid',
        python_callable=find_uuid,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                 ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )

        
    t_run_md_extract = BashOperator(
        task_id='run_md_extract',
        bash_command=""" \
        lz_dir="{{ti.xcom_pull(task_ids='find_uuid', key='lz_path')}}" \
        src_dir="{{get_src_path()}}/md" ; \
        top_dir="{{get_src_path()}}" ; \
        cd "$lz_dir" ; \
        env PYTHONPATH=${PYTHONPATH}:$top_dir \
        python $src_dir/metadata_extract.py --out /dev/null "$lz_dir" \
          > session.log 2> error.log ; \
        echo $? ; \
        if [ -s error.log ] ; \
        then echo 'ERROR!' `cat error.log` >> session.log ; \
        else rm error.log ; \
        fi
        """
        )


    (dag >> t_find_uuid >> t_run_md_extract)

