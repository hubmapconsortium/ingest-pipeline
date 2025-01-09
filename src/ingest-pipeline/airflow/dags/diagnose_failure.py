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

import diagnostics.diagnostic_plugin as diagnostic_plugin

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

        for key in ['entity_type', 'status', 'uuid', 'data_types',
                    'local_directory_full_path']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if not ds_rslt['entity_type'] in ['Dataset']:
            raise AirflowException(f'Entity {uuid} is not a Dataset')

        if not ds_rslt['status'] in ['Error']:
            raise AirflowException(f'Dataset {uuid} is not in Error state')

        if ('parent_dataset_uuid_list' in ds_rslt
                and ds_rslt['parent_dataset_uuid_list'] is not None):
            parent_dataset_full_path_list = []
            parent_dataset_data_types_list = []
            parent_dataset_data_path_list = []
            for parent_uuid in ds_rslt['parent_dataset_uuid_list']:
                def parent_callable(**kwargs):
                    return parent_uuid
                parent_ds_rslt = utils.pythonop_get_dataset_state(
                    dataset_uuid_callable=parent_callable,
                    **kwargs
                )
                if not parent_ds_rslt:
                    raise AirflowException(f'Invalid uuid for parent: {parent_uuid}')
                parent_dataset_full_path_list.append(
                    parent_ds_rslt['local_directory_full_path']
                )
                parent_dataset_data_types_list.append(
                    parent_ds_rslt['data_types']
                )
                if ('metadata' in parent_ds_rslt
                        and 'data_path' in parent_ds_rslt['metadata']):
                    parent_dataset_data_path_list.append(parent_ds_rslt['metadata']['data_path'])
                else:
                    parent_dataset_data_path_list.append(None)
            ds_rslt['parent_dataset_full_path_list'] = parent_dataset_full_path_list
            ds_rslt['parent_dataset_data_types_list'] = parent_dataset_data_types_list
            ds_rslt['parent_dataset_data_path_list'] = parent_dataset_data_path_list

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
        plugin_path = Path(diagnostic_plugin.__file__).parent / 'plugins'
        for plugin in diagnostic_plugin.diagnostic_result_iter(plugin_path, **info_dict):
            diagnostic_result = plugin.diagnose()
            if diagnostic_result.problem_found():
                logging.info(f'Plugin "{plugin.description}" found problems:')
                for err_str in diagnostic_result.to_strings():
                    logging.info("    " + err_str)
            else:
                logging.info(f'Plugin "{plugin.description}" found no problem')
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
