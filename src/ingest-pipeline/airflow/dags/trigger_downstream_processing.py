import ast
from pprint import pprint
from datetime import datetime, timedelta

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator
from airflow.exceptions import AirflowException

import utils
from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    HMDAG,
    get_queue_resource,
    )


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
    'queue': get_queue_resource('trigger_downstream_processing'),
}


with HMDAG(
            'trigger_downstream_processing',
            schedule_interval=None,
            is_paused_upon_creation=False,
            default_args=default_args,
       ) as dag:

    def find_uuid(**kwargs):
        try:
            assert_json_matches_schema(kwargs['dag_run'].conf,
                                       'trigger_downstream_processing_schema.yml')
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
        print(f'ds_rslt: {ds_rslt}')

        for key in ['uuid', 'data_types', 'local_directory_full_path']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

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
                raise AirflowException(f'Dataset data_types for {uuid}'
                                       ' is empty')
        else:
            filtered_data_types = [dt]

        lz_path = ds_rslt['local_directory_full_path']
        uuid = ds_rslt['uuid']  # 'uuid' may  actually be a DOI
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
            'crypt_auth_tok': (
                utils.encrypt_tok(airflow_conf.as_dict()
                                  ['connections']['APP_CLIENT_SECRET'])
                .decode()
                ),
            }
        )

    def flex_maybe_spawn(**kwargs):
        """
        This is a generator which returns appropriate DagRunOrders
        """
        print(f'kwargs: {kwargs}')
        ctx = kwargs['dag_run'].conf
        print(f'dag_run conf: {ctx}')
        collectiontype = 'generic_metadatatsv'  # always the case now
        assay_type = kwargs['ti'].xcom_pull(key='assay_type',
                                            task_ids="find_uuid")
        assay_type = assay_type[0]
        print('collectiontype: <{}>, assay_type: <{}>'.format(collectiontype, assay_type))
        # payload = {k: kwargs['dag_run'].conf[k] for k in kwargs['dag_run'].conf}
        payload = {'ingest_id': kwargs['run_id'],
                   'crypt_auth_tok': kwargs['crypt_auth_tok'],
                   'parent_lz_path': kwargs['ti'].xcom_pull(key='lz_path',
                                                            task_ids="find_uuid"),
                   'parent_submission_id': kwargs['ti'].xcom_pull(key='uuid',
                                                                  task_ids="find_uuid"),
                   'dag_provenance_list': utils.get_git_provenance_list(__file__)
                   }
        for next_dag in utils.downstream_workflow_iter(collectiontype, assay_type):
            yield next_dag, {payload}


    t_maybe_spawn = FlexMultiDagRunOperator(
        task_id='flex_maybe_spawn',
        provide_context=True,
        python_callable=flex_maybe_spawn,
        op_kwargs={
            'crypt_auth_tok': (
                utils.encrypt_tok(airflow_conf.as_dict()
                                  ['connections']['APP_CLIENT_SECRET'])
                .decode()
                ),
            }
        )

    t_find_uuid >> t_maybe_spawn
