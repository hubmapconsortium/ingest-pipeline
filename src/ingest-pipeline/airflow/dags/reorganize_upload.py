import sys
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook

from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

from utils import (
    pythonop_maybe_keep,
    get_tmp_dir_path, get_auth_tok,
    pythonop_get_dataset_state,
    pythonop_set_dataset_state,
    find_matching_endpoint,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_instance_type,
    get_environment_instance
)

from aws_utils import (
    create_instance,
    terminate_instance
)

from misc.tools.split_and_create import reorganize

sys.path.append(airflow_conf.as_dict()['connections']['SRC_PATH']
                .strip("'").strip('"'))
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
    'queue': get_queue_resource('reorganize_upload'),
}


def _get_frozen_df_path(run_id):
    # This version of the path is passed to the internals of
    # split_and_create, and must contain formatting space for
    # a suffix.
    return str(Path(get_tmp_dir_path(run_id)) / 'frozen_source_df{}.tsv')


def _get_frozen_df_wildcard(run_id):
    # This version of the path is used from a bash command line
    # and must match all frozen_df files regardless of suffix.
    return str(Path(get_tmp_dir_path(run_id)) / 'frozen_source_df*.tsv')


with HMDAG('reorganize_upload',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'frozen_df_path': _get_frozen_df_path,
               'frozen_df_wildcard': _get_frozen_df_wildcard,
               'preserve_scratch': get_preserve_scratch_resource('reorganize_upload'),
           }) as dag:
    def start_new_environment(**kwargs):
        uuid = kwargs['dag_run'].conf['uuid']
        instance_id = create_instance(uuid, f'Airflow {get_environment_instance()} Worker',
                                      get_instance_type(kwargs['dag_run'].conf['dag_id']))
        if instance_id is None:
            return 1
        else:
            kwargs['ti'].xcom_push(key='instance_id', value=instance_id)
            return 0


    t_initialize_environment = PythonOperator(
        task_id='initialize_environment',
        python_callable=start_new_environment,
        provide_context=True,
        op_kwargs={
        }
    )

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
            raise AirflowException(f"status of Upload {uuid} is not Valid, or Processing, {ds_rslt['status']}")

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
        frozen_df_wildcard="{{frozen_df_wildcard(run_id)}}" ; \
        upload_path="{{ti.xcom_pull(task_ids="find_uuid", key="lz_path")}}" ; \
        if ls $frozen_df_wildcard > /dev/null 2>&1 ; then \
          cp ${frozen_df_wildcard} "${upload_path}" ; \
        fi
        """
    )


    def split_stage_1(**kwargs):
        uuid = kwargs['ti'].xcom_pull(task_ids='find_uuid', key='uuid')
        entity_host = HttpHook.get_connection('entity_api_connection').host
        try:
            reorganize(
                uuid,
                mode='stop',
                ingest=False,
                # dryrun=True,
                dryrun=False,
                instance=find_matching_endpoint(entity_host),
                auth_tok=get_auth_tok(**kwargs),
                frozen_df_fname=_get_frozen_df_path(kwargs['run_id'])
            )
            kwargs['ti'].xcom_push(key='split_stage_1', value='0')  # signal success
        except Exception as e:
            print(f'Encountered {e}')
            kwargs['ti'].xcom_push(key='split_stage_1', value='1')  # signal failure


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
                # dryrun=True,
                dryrun=False,
                instance=find_matching_endpoint(entity_host),
                auth_tok=get_auth_tok(**kwargs),
                frozen_df_fname=_get_frozen_df_path(kwargs['run_id'])
            )
            kwargs['ti'].xcom_push(key='split_stage_2', value='0')  # signal success
        except Exception as e:
            print(f'Encountered {e}')
            kwargs['ti'].xcom_push(key='split_stage_2', value='1')  # signal failure


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
        python_callable=pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule='all_done',
        op_kwargs={
            'dataset_uuid_callable': _get_upload_uuid,
            'ds_state': 'Error'
        }
    )


    def terminate_new_environment(**kwargs):
        instance_id = kwargs['ti'].xcom_pull(key='instance_id', task_ids="initialize_environment")
        if instance_id is None:
            return 1
        else:
            uuid = kwargs['dag_run'].conf['uuid']
            terminate_instance(instance_id, uuid)
        return 0


    t_terminate_environment = PythonOperator(
        task_id='terminate_environment',
        python_callable=terminate_new_environment,
        provide_context=True,
        op_kwargs={
        }
    )

    (
        t_initialize_environment
        >> t_log_info
        >> t_find_uuid
        >> t_create_tmpdir
        >> t_split_stage_1
        >> t_maybe_keep_1
        >> t_split_stage_2
        >> t_maybe_keep_2
        >> t_join
        >> t_preserve_info
        >> t_cleanup_tmpdir
        >> t_terminate_environment
     )

    t_maybe_keep_1 >> t_set_dataset_error
    t_maybe_keep_2 >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_terminate_environment
