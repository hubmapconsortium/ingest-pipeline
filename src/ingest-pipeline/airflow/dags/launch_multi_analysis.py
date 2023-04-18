import ast
from pprint import pprint
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.configuration import conf as airflow_conf
from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator

import utils

from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
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
    'queue': get_queue_resource('launch_multi_analysis'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error)
}


with HMDAG('launch_multi_analysis',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': utils.get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('launch_multi_analysis')
           }) as dag:

    def start_new_environment(**kwargs):
        uuid = kwargs['dag_run'].conf['submission_id']
        instance_id = create_instance(uuid, f'Airflow {get_environment_instance()} Worker',
                                      get_instance_type(kwargs.get('dag_id')))
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

    def check_one_uuid(uuid, **kwargs):
        """
        Look up information on the given uuid or HuBMAP identifier.
        Returns:
        - the uuid, translated from an identifier if necessary
        - data type(s) of the dataset
        - local directory full path of the dataset
        """
        print(f'Starting uuid {uuid}')
        my_callable = lambda **kwargs: uuid
        ds_rslt = utils.pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f'Invalid uuid/doi for group: {uuid}')
        print('ds_rslt:')
        pprint(ds_rslt)

        for key in ['status', 'uuid', 'data_types', 'local_directory_full_path',
                    'metadata']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if not ds_rslt['status'] in ['New', 'Error', 'QA', 'Published']:
            raise AirflowException(f'Dataset {uuid} is not QA or better')

        dt = ds_rslt['data_types']
        if isinstance(dt, str) and dt.startswith('[') and dt.endswith(']'):
            dt = ast.literal_eval(dt)
            print(f'parsed dt: {dt}')

        return (ds_rslt['uuid'], dt, ds_rslt['local_directory_full_path'],
                ds_rslt['metadata'])


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
        prev_version_uuid = kwargs['dag_run'].conf.get('previous_version_uuid',
                                                       None)
        filtered_uuid_l = []
        filtered_path_l = []
        filtered_data_types = []
        filtered_md_l = []
        for uuid in uuid_l:
            uuid, dt, lz_path, metadata = check_one_uuid(uuid, **kwargs)
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

            filtered_path_l.append(lz_path)
            filtered_uuid_l.append(uuid)
            filtered_md_l.append(metadata)
        if prev_version_uuid is not None:
            prev_version_uuid = check_one_uuid(prev_version_uuid, **kwargs)[0]
        # print(f'Finished uuid {uuid}')
        print(f'filtered data types: {filtered_data_types}')
        print(f'filtered paths: {filtered_path_l}')
        print(f'filtered uuids: {filtered_uuid_l}')
        kwargs['ti'].xcom_push(key='collectiontype', value=collection_type)
        kwargs['ti'].xcom_push(key='assay_type', value=filtered_data_types)
        kwargs['ti'].xcom_push(key='lz_paths', value=filtered_path_l)
        kwargs['ti'].xcom_push(key='uuids', value=filtered_uuid_l)
        kwargs['ti'].xcom_push(key='metadata_list', value=filtered_md_l)
        kwargs['ti'].xcom_push(key='previous_version_uuid', value=prev_version_uuid)

    check_uuids_t = PythonOperator(
        task_id='check_uuids',
        python_callable=check_uuids,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': utils.encrypt_tok(airflow_conf.as_dict()
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
        metadata_list = kwargs['ti'].xcom_pull(key='metadata_list', task_ids="check_uuids")
        prev_version_uuid = kwargs['ti'].xcom_pull(key='previous_version_uuid',
                                                   task_ids="check_uuids")
        print('collectiontype: <{}>, assay_type: <{}>'.format(collectiontype, assay_type))
        print(f'uuids: {uuids}')
        print('lz_paths:')
        pprint(lz_paths)
        print(f'previous version uuid: {prev_version_uuid}')
        payload = {
                    'ingest_id': kwargs['run_id'],
                    'crypt_auth_tok': kwargs['crypt_auth_tok'],
                    'parent_lz_path': lz_paths,
                    'parent_submission_id': uuids,
                    'previous_version_uuid': prev_version_uuid,
                    'metadata': metadata_list,
                    'dag_provenance_list': utils.get_git_provenance_list(__file__)
        }
        for next_dag in utils.downstream_workflow_iter(collectiontype, assay_type):
            yield next_dag, payload

    t_maybe_spawn = FlexMultiDagRunOperator(
        task_id='flex_maybe_spawn',
        provide_context=True,
        python_callable=flex_maybe_spawn,
        op_kwargs={
            'crypt_auth_tok': utils.encrypt_tok(airflow_conf.as_dict()
                                                ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )

    def terminate_new_environment(**kwargs):
        instance_id = kwargs['ti'].xcom_pull(key='instance_id', task_ids="initialize_environment")
        if instance_id is None:
            return 1
        else:
            uuid = kwargs['dag_run'].conf['submission_id']
            terminate_instance(instance_id, uuid)
        return 0

    t_terminate_environment = PythonOperator(
        task_id='terminate_environment',
        python_callable=terminate_new_environment,
        provide_context=True,
        op_kwargs={
        }
    )

    t_initialize_environment >> check_uuids_t >> t_maybe_spawn >> t_terminate_environment
