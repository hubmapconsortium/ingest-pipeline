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
from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator

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


with DAG('launch_multi_analysis', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         max_active_runs=1,
         user_defined_macros={'tmp_dir_path' : utils.get_tmp_dir_path}
         ) as dag:

    def check_uuids(**kwargs):
        print('dag_run conf follows:')
        pprint(kwargs['dag_run'].conf)

        try:
            assert_json_matches_schema(kwargs['dag_run'].conf,
                                       'launch_multi_metadata_schema.yml')
        except AssertionError as e:
            print('invalid metadata follows:')
            pprint(kwargs['dag_run'].conf)
            raise
        
        uuid_l = kwargs['dag_run'].conf['uuid_list']
        collection_type = kwargs['dag_run'].conf['collection_type']
        filtered_uuid_l = []
        filtered_path_l = []
        filtered_data_types = []
        for uuid in uuid_l:
            print(f'Starting uuid {uuid}')
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

            if not ds_rslt['status'] in ['QA', 'Published']:
                raise AirflowException(f'Dataset {uuid} is not QA or better')

            dt = ds_rslt['data_types']
            if isinstance(dt, str) and dt.startswith('[') and dt.endswith(']'):
                dt = ast.literal_eval(dt)
            print(f'parsed dt: {dt}')
            if isinstance(dt, list):
                if dt:
                    if len(dt) == 1:
                        filtered_data_types.append(dt[0])
                    else:
                        filtered_data_types.append(tuple(dt))
                else:
                    raise AirflowException(f'Dataset data_types for {uuid} is empty')
            else:
                filtered_data_types.append(dt)

            lz_path = ds_rslt['local_directory_full_path']
            filtered_path_l.append(lz_path)
            filtered_uuid_l.append(ds_rslt['uuid'])
        print(f'Finished uuid {uuid}')
        print(f'filtered data types: {filtered_data_types}')
        print(f'filtered paths: {filtered_path_l}')
        print(f'filtered uuids: {filtered_uuid_l}')
        kwargs['ti'].xcom_push(key='collectiontype', value=collection_type)
        kwargs['ti'].xcom_push(key='assay_type', value=filtered_data_types)
        kwargs['ti'].xcom_push(key='lz_paths', value=filtered_path_l)
        kwargs['ti'].xcom_push(key='uuids', value=filtered_uuid_l)

    check_uuids_t = PythonOperator(
        task_id='check_uuids',
        python_callable=check_uuids,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                 ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )

    def flex_maybe_spawn(**kwargs):
        """
        This is a generator which returns appropriate DagRunOrders
        """
        print('kwargs:')
        pprint(kwargs)
        print('dag_run conf:')
        ctx = kwargs['dag_run'].conf
        pprint(ctx)
        collectiontype = kwargs['ti'].xcom_pull(key='collectiontype', task_ids="check_uuids")
        assay_type = kwargs['ti'].xcom_pull(key='assay_type', task_ids="check_uuids")
        lz_paths = kwargs['ti'].xcom_pull(key='lz_paths', task_ids="check_uuids")
        uuids = kwargs['ti'].xcom_pull(key='uuids', task_ids="check_uuids")
        print('collectiontype: <{}>, assay_type: <{}>'.format(collectiontype, assay_type))
        print(f'uuids: {uuids}')
        print('lz_paths:')
        pprint(lz_paths)
        payload = {'ingest_id' : kwargs['run_id'],
                   'crypt_auth_tok' : kwargs['crypt_auth_tok'],
                   'parent_lz_path' : lz_paths,
                   'parent_submission_id' : uuids,
                   'metadata': {},
                   'dag_provenance_list' : utils.get_git_provenance_list(__file__)
        }
        for next_dag in utils.downstream_workflow_iter(collectiontype, assay_type):
            yield next_dag, DagRunOrder(payload=payload)

    t_maybe_spawn = FlexMultiDagRunOperator(
        task_id='flex_maybe_spawn',
        provide_context=True,
        python_callable=flex_maybe_spawn,
        op_kwargs={
            'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                 ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )

    dag >> check_uuids_t >> t_maybe_spawn

