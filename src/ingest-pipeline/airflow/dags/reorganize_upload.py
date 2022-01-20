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
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook

from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
    SetDatasetProcessingOperator
)

import utils
from utils import (
    pythonop_maybe_keep,
    get_tmp_dir_path, get_auth_tok,
    map_queue_name, pythonop_get_dataset_state,
    localized_assert_json_matches_schema as assert_json_matches_schema
    )

sys.path.append(airflow_conf.as_dict()['connections']['SRC_PATH']
                .strip("'").strip('"'))
from misc.tools.split_and_create import reorganize
from misc.tools.survey import ENDPOINTS
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


def _get_frozen_df_path(run_id):
    return Path(get_tmp_dir_path(run_id)) / 'frozen_source_df.tsv'


with DAG('reorganize_upload',
         schedule_interval=None,
         is_paused_upon_creation=False,
         user_defined_macros={
             'tmp_dir_path' : get_tmp_dir_path,
             'frozen_df_path' : _get_frozen_df_path
         },
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
        if ds_rslt['status'] not in ['Valid', 'Processing']:
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

    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')

    t_preserve_info = BashOperator(
        task_id='preserve_info',
        bash_command="""
        frozen_df="{{frozen_df_path(run_id)}}" ; \
        upload_path="{{ti.xcom_pull(task_ids="find_uuid", key="lz_path")}}" ; \
        if [ -e ${frozen_df} ] ; then \
          cp ${frozen_df} "${upload_path}" ; \
        fi
        """
    )

    def _strip_url(url):
        return url.split(':')[1].strip('/')

    def _find_matching_endpoint(host_url):
        stripped_url = _strip_url(host_url)
        print(f'stripped_url: {stripped_url}')
        candidates = [ep for ep in ENDPOINTS
                      if stripped_url == _strip_url(ENDPOINTS[ep]['entity_url'])]
        assert len(candidates) == 1, f'Found {candidates}, expected 1 match'
        return candidates[0]

    def split_stage_1(**kwargs):
        uuid = kwargs['ti'].xcom_pull(task_ids='find_uuid', key='uuid')
        entity_host = HttpHook.get_connection('entity_api_connection').host
        try:
            reorganize(
                uuid,
                mode='stop',
                ingest=False,
                #dryrun=True,
                instance=_find_matching_endpoint(entity_host),
                auth_tok=get_auth_tok(**kwargs),
                frozen_df_fname=_get_frozen_df_path(kwargs['run_id'])
            )
            kwargs['ti'].xcom_push(key=None, value='0')  # signal success
        except Exception as e:
            print(f'Encountered {e}')
            kwargs['ti'].xcom_push(key=None, value='1')  # signal failure

    t_split_stage_1 = PythonOperator(
        task_id='split_stage_1',
        python_callable=split_stage_1,
        provide_context=True,
        op_kwargs={
            }
        )

    t_maybe_keep_1 = BranchPythonOperator(
        task_id='maybe_keep_1',
        python_callable=pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            'next_op': 'split_stage_2',
            'bail_op': 'set_dataset_error',
            'test_op': 'split_stage_1'
            }
    )

    def split_stage_2(**kwargs):
        uuid = kwargs['ti'].xcom_pull(task_ids='find_uuid', key='uuid')
        entity_host = HttpHook.get_connection('entity_api_connection').host
        try:
            reorganize(
                uuid,
                mode='unstop',
                ingest=False,
                #dryrun=True,
                instance=_find_matching_endpoint(entity_host),
                auth_tok=get_auth_tok(**kwargs),
                frozen_df_fname=_get_frozen_df_path(kwargs['run_id'])
            )
            kwargs['ti'].xcom_push(key=None, value='0')  # signal success
        except Exception as e:
            print(f'Encountered {e}')
            kwargs['ti'].xcom_push(key=None, value='1')  # signal failure

    t_split_stage_2 = PythonOperator(
        task_id='split_stage_2',
        python_callable=split_stage_2,
        provide_context=True,
        op_kwargs={
            }
        )

    t_maybe_keep_2 = BranchPythonOperator(
        task_id='maybe_keep_2',
        python_callable=pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            'next_op': 'join',
            'bail_op': 'set_dataset_error',
            'test_op': 'split_stage_2'
            }
    )

    t_log_info = LogInfoOperator(task_id='log_info')

    t_join = JoinOperator(task_id='join')

    def _get_upload_uuid(**kwargs):
        return kwargs['ti'].xcom_pull(task_ids='find_uuid', key='uuid')

    t_set_dataset_error = PythonOperator(
        task_id='set_dataset_error',
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule='all_done',
        op_kwargs = {
            'dataset_uuid_callable' : _get_upload_uuid,
            'ds_state' : 'Error'
        }
    )

    (dag
     >> t_log_info
     >> t_find_uuid
     >> t_create_tmpdir 
     >> t_split_stage_1
     >> t_maybe_keep_1
     >> t_split_stage_2
     >> t_maybe_keep_2
     >> t_join
     >> t_preserve_info
     >> t_cleanup_tmpdir)

    t_maybe_keep_1 >> t_set_dataset_error
    t_maybe_keep_2 >> t_set_dataset_error
    t_set_dataset_error >> t_join
