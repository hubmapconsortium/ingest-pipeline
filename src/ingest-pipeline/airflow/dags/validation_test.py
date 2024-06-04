import sys
import os
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

import utils
from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    HMDAG,
    get_queue_resource,
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
    'queue': get_queue_resource('validation_test'),
    "executor_config": {"SlurmExecutor": {"slurm_output_path": "/hive/users/hive/airflow-logs/slurm/%x_%N_%j.out"}},
}

with HMDAG('validation_test',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           ) as dag:

    def find_uuid(**kwargs):
        try:
            assert_json_matches_schema(kwargs['dag_run'].conf,
                                       'validation_test_schema.yml')
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
        print('ds_rslt:')
        pprint(ds_rslt)

        for key in ['entity_type', 'status', 'uuid', 'data_types',
                    'local_directory_full_path']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if not ds_rslt['entity_type'] in ['Dataset', 'Upload', 'Publication']:
            raise AirflowException(f'Entity {uuid} is not a Dataset,'
                                   ' Publication, or Upload')

        if ds_rslt['entity_type'] in ['Dataset', 'Publication']:
            if not ds_rslt['status'] in ['New', 'Invalid']:
                raise AirflowException(f'Dataset {uuid} is not New or Invalid')
        else:
            if not ds_rslt['status'] in ['New', 'Submitted', 'Invalid']:
                raise AirflowException(f'Upload {uuid} is not New, Submitted, or Invalid')

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
            'crypt_auth_tok': (
                utils.encrypt_tok(airflow_conf.as_dict()
                                  ['connections']['APP_CLIENT_SECRET'])
                .decode()
                ),
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
            # offline=True,  # noqa E265
            add_notes=False
        )
        # Scan reports an error result
        report = ingest_validation_tools_error_report.ErrorReport(
            errors=upload.get_errors(plugin_kwargs=kwargs),
            info=upload.get_info()
        )
        with open(os.path.join(lz_path, 'validation_report.txt'), 'w') as f:
            f.write(report.as_text())

    t_run_validation = PythonOperator(
        task_id='run_validation',
        python_callable=run_validation,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': (
                utils.encrypt_tok(airflow_conf.as_dict()
                                  ['connections']['APP_CLIENT_SECRET'])
                .decode()
            ),
        }
    )

    t_find_uuid >> t_run_validation
