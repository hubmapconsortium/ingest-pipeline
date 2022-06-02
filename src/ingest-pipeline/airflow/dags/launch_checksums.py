import json
import ast
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

import pandas as pd

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
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
    get_auth_tok,
    get_tmp_dir_path,
)


# How many records to send to uuid-api in each block
RECS_PER_BLOCK = 100


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
}


with HMDAG('launch_checksums',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path' : utils.get_tmp_dir_path,
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
        except AssertionError:
            print('invalid metadata follows:')
            pprint(kwargs['dag_run'].conf)
            raise

        uuid = kwargs['dag_run'].conf['uuid']
        filtered_uuid = None
        filtered_path = None
        filtered_data_type = None
        print(f'Starting uuid {uuid}')
        my_callable = lambda **kwargs: uuid
        rslt = utils.pythonop_get_dataset_state(dataset_uuid_callable=my_callable,
                                                **kwargs
                                                )
        if not rslt:
            raise AirflowException(f'Invalid uuid/doi: {uuid}')
        print('rslt:')
        pprint(rslt)

        for key in ['status', 'uuid', 'data_types', 'local_directory_full_path']:
            assert key in rslt, f"Dataset status for {uuid} has no {key}"

#         if not rslt['status'] in ['Published']:
#             raise AirflowException(f'Dataset {uuid} is not QA or better')

        data_types = rslt['data_types']
        if isinstance(data_types, str) and data_types.startswith('[') and data_types.endswith(']'):
            data_types = ast.literal_eval(data_types)
        print(f'parsed data_types: {data_types}')
        if isinstance(data_types, list):
            if data_types:
                if len(data_types) == 1:
                    filtered_data_type = data_types[0]
                else:
                    filtered_data_type = tuple(data_types)
            else:
                raise AirflowException(f'Dataset data_types for {uuid} is empty')
        else:
            filtered_data_type = data_types

        lz_path = rslt['local_directory_full_path']
        filtered_path = lz_path
        filtered_uuid = rslt['uuid']
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
        python "$src_path"/misc/tools/checksum_sha256.py \
            -i "${ds_dir}" -o "${tmp_dir}"/cksums.tsv \
            -n "{{THREADS}}"
        """
        )


    def send_block(parent_uuid, parent_path, block_df, **kwargs):
        headers = {
            'authorization' : 'Bearer ' + get_auth_tok(**kwargs),
            'content-type' : 'application/json',
            'X-Hubmap-Application' : 'ingest-pipeline'
        }
        rec_l = []
        for idx, row in block_df.iterrows():  # pylint: disable=unused-variable
            this_path = Path(row['path'])
            rec_l.append({
                'path':str(this_path.relative_to(parent_path.parent)),
                'checksum':row['sha256'],
                'size':row['size'],
                'base_dir':'DATA_UPLOAD'
            })
        data = {
            'entity_type':'FILE',
            'parent_ids':[parent_uuid],
            'file_info':rec_l
            }
        print('sending the following payload:')
        pprint(data)
        response = HttpHook('POST', http_conn_id='uuid_api_connection').run(
            endpoint=f'hmuuid?entity_count={len(rec_l)}',
            data=json.dumps(data),
            headers=headers,
            extra_options=[]
            )
        response.raise_for_status()
        print('response block follows')
        pprint(response.json())


    def send_checksums(**kwargs):
        run_id = kwargs['run_id']
        tmp_dir_path = get_tmp_dir_path(run_id)
        parent_uuid = kwargs['ti'].xcom_pull(task_ids='check_uuid', key='uuid')
        parent_path = Path(kwargs['ti'].xcom_pull(task_ids='check_uuid', key='lz_path'))

        full_df = pd.read_csv(Path(tmp_dir_path) / 'cksums.tsv', sep='\t')
        tot_recs = len(full_df)
        low_rec = 0
        while low_rec < tot_recs:
            block_df = full_df.iloc[low_rec : low_rec + RECS_PER_BLOCK]
            send_block(parent_uuid, parent_path, block_df, **kwargs)
            low_rec += RECS_PER_BLOCK


    t_send_checksums = PythonOperator(
        task_id='send_checksums',
        python_callable=send_checksums,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok' : utils.encrypt_tok(airflow_conf.as_dict()
                                                 ['connections']['APP_CLIENT_SECRET']).decode(),
            }
        )

    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmpdir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmpdir')

    (dag >> t_create_tmpdir
     >> check_uuid_t
     >> t_gen_cksum_table
     >> t_send_checksums
     >> t_cleanup_tmpdir
     )
