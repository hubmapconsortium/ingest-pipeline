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
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook

from utils import (
    get_tmp_dir_path, get_auth_tok,
    map_queue_name, pythonop_get_dataset_state,
    localized_assert_json_matches_schema as assert_json_matches_schema
    )

sys.path.append(airflow_conf.as_dict()['connections']['SRC_PATH']
                .strip("'").strip('"'))
from submodules import (ingest_validation_tools_upload,  # noqa E402
                        ingest_validation_tools_error_report,
                        ingest_validation_tests)
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


with DAG('validate_upload',
         schedule_interval=None,
         is_paused_upon_creation=False,
         user_defined_macros={'tmp_dir_path' : get_tmp_dir_path},
         default_args=default_args,
         ) as dag:

    def find_uuid(**kwargs):
        uuid = kwargs['dag_run'].conf['uuid']

        def my_callable(**kwargs):
            return uuid

        ds_rslt = pythonop_get_dataset_state(
            dataset_uuid_callable=my_callable,
            http_conn_id='ingest_api_connection',
            **kwargs
        )
        if not ds_rslt:
            raise AirflowException(f'Invalid uuid/doi for group: {uuid}')
        print('ds_rslt:')
        pprint(ds_rslt)

        for key in ['status', 'uuid', 'data_types',
                    'local_directory_full_path']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if False:  # not ds_rslt['status'] in ['Processing']:
            raise AirflowException(f'Dataset {uuid} is not Processing')

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
            }
        )

    def run_validation(**kwargs):
        lz_path = kwargs['ti'].xcom_pull(key='lz_path')
        uuid = kwargs['ti'].xcom_pull(key='uuid')
        plugin_path = [path for path in ingest_validation_tests.__path__][0]

        ignore_globs = [uuid, 'extras', '*metadata.tsv',
                        'validation_report.txt']
        #
        # Uncomment offline=True below to avoid validating orcid_id URLs &etc
        #
        upload = ingest_validation_tools_upload.Upload(
            directory_path=Path(lz_path),
            dataset_ignore_globs=ignore_globs,
            upload_ignore_globs='*',
            plugin_directory=plugin_path,
            #offline=True,  # noqa E265
            add_notes=False
        )
        # Scan reports an error result
        report = ingest_validation_tools_error_report.ErrorReport(
            upload.get_errors(plugin_kwargs=kwargs)
        )
        validation_file_path = Path(get_tmp_dir_path(kwargs['run_id'])) / 'validation_report.txt'
        with open(validation_file_path, 'w') as f:
            f.write(report.as_text())
        kwargs['ti'].xcom_push(key='validation_file_path', value=str(validation_file_path))

    t_run_validation = PythonOperator(
        task_id='run_validation',
        python_callable=run_validation,
        provide_context=True,
        queue=utils.map_queue_name('validate'),
        op_kwargs={
        }
    )

    def send_status_msg(**kwargs):
        validation_file_path = Path(kwargs['ti'].xcom_pull(key='validation_file_path'))
        uuid = kwargs['ti'].xcom_pull(key='uuid')
        conn_id = ''
        endpoint = f'/entities/{uuid}'
        headers = {
            'authorization': 'Bearer ' + get_auth_tok(**kwargs),
            'X-Hubmap-Application':'ingest-pipeline',
            'content-type': 'application/json',
        }
        extra_options = []
        http_conn_id='entity_api_connection'
        http_hook = HttpHook('PUT', http_conn_id=http_conn_id)
        with open(validation_file_path) as f:
            report_txt = f.read()
        if report_txt.startswith('No errors!'):
            data = {
                "status":"Valid",
            }       
        else:
            data = {
                "status":"Invalid",
                "validation_message" : report_txt
            }       
        print('data: ')
        pprint(data)
        response = http_hook.run(
            endpoint,
            json.dumps(data),
            headers,
            extra_options,
        )
        print('response: ')
        pprint(response.json())


    
    t_send_status = PythonOperator(
        task_id='send_status',
        python_callable=send_status_msg,
        provide_context=True,
        op_kwargs={
        }        
    )

    t_create_tmpdir = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir {{tmp_dir_path(run_id)}}'
        )

    t_cleanup_tmpdir = BashOperator(
        task_id='cleanup_temp_dir',
        bash_command='rm -r {{tmp_dir_path(run_id)}}',
        trigger_rule='all_success'
        )

    (dag >> t_create_tmpdir >> t_find_uuid >> t_run_validation 
     >> t_send_status >> t_cleanup_tmpdir)
