import os
import yaml
import utils
from pprint import pprint

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.configuration import conf as airflow_conf
from datetime import datetime, timedelta

from utils import (
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    create_dataset_state_error_callback,
    pythonop_md_consistency_tests,
    make_send_status_msg_function,
    get_tmp_dir_path,
    localized_assert_json_matches_schema as assert_json_matches_schema,
    pythonop_get_dataset_state,
    encrypt_tok,
)

from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)


def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    return None


def get_dataset_uuid(**kwargs):
    return kwargs['dag_run'].conf['uuid']


def get_dataset_lz_path(**kwargs):
    ctx = kwargs['dag_run'].conf
    return ctx['lz_path']


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
    'queue': get_queue_resource('rebuild_metadata'),
    'on_failure_callback': create_dataset_state_error_callback(get_uuid_for_error)
}

with HMDAG('rebuild_primary_dataset_metadata',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('rebuild_metadata')
           }) as dag:

    t_create_tmpdir = CreateTmpDirOperator(task_id='create_temp_dir')

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
        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f'Invalid uuid/doi for group: {uuid}')
        print('ds_rslt:')
        pprint(ds_rslt)

        for key in ['status', 'uuid', 'local_directory_full_path', 'metadata']:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if not ds_rslt['status'] in ['New', 'Error', 'QA', 'Published']:
            raise AirflowException(f'Dataset {uuid} is not QA or better')

        return (ds_rslt['uuid'], ds_rslt['local_directory_full_path'],
                ds_rslt['metadata'])

    def check_uuids(**kwargs):
        print('dag_run conf follows:')
        pprint(kwargs['dag_run'].conf)

        try:
            assert_json_matches_schema(kwargs['dag_run'].conf,
                                       'launch_checksums_metadata_schema.yml')
        except AssertionError as e:
            print('invalid metadata follows:')
            pprint(kwargs['dag_run'].conf)
            raise

        uuid, lz_path, metadata = check_one_uuid(kwargs['dag_run'].conf['uuid'], **kwargs)
        print(f'filtered metadata: {metadata}')
        print(f'filtered paths: {lz_path}')
        kwargs['dag_run'].conf['lz_path'] = lz_path
        kwargs['dag_run'].conf['src_path'] = airflow_conf.as_dict()['connections']['src_path'].strip("'")


    t_check_uuids = PythonOperator(
        task_id='check_uuids',
        python_callable=check_uuids,
        provide_context=True,
        op_kwargs={
            'crypt_auth_tok': encrypt_tok(airflow_conf.as_dict()
                                          ['connections']['APP_CLIENT_SECRET']).decode(),
        }
    )

    t_run_md_extract = BashOperator(
        task_id='run_md_extract',
        bash_command=""" \
            lz_dir="{{dag_run.conf.lz_path}}" ; \
            src_dir="{{dag_run.conf.src_path}}/md" ; \
            top_dir="{{dag_run.conf.src_path}}" ; \
            work_dir="{{tmp_dir_path(run_id)}}" ; \
            cd $work_dir ; \
            env PYTHONPATH=${PYTHONPATH}:$top_dir \
            ${PYTHON_EXE} $src_dir/metadata_extract.py --out ./rslt.yml --yaml "$lz_dir" \
              >> session.log 2> error.log ; \
            echo $? ; \
            if [ -s error.log ] ; \
            then echo 'ERROR!' `cat error.log` >> session.log ; \
            else rm error.log ; \
            fi
            """,
        env={
            'AUTH_TOK': (
                utils.get_auth_tok(
                    **{
                        'crypt_auth_tok': utils.encrypt_tok(
                            airflow_conf.as_dict()['connections']['APP_CLIENT_SECRET']).decode()
                    }
                )
            ),
            'PYTHON_EXE': os.environ["CONDA_PREFIX"] + "/bin/python",
            'INGEST_API_URL': os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]
        }
    )

    t_md_consistency_tests = PythonOperator(
        task_id='md_consistency_tests',
        python_callable=pythonop_md_consistency_tests,
        provide_context=True,
        op_kwargs={'metadata_fname': 'rslt.yml'}
    )

    def read_metadata_file(**kwargs):
        md_fname = os.path.join(get_tmp_dir_path(kwargs['run_id']),
                                'rslt.yml')
        with open(md_fname, 'r') as f:
            scanned_md = yaml.safe_load(f)
        return scanned_md

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=['run_md_extract', 'md_consistency_tests'],
        cwl_workflows=[],
        dataset_uuid_fun=get_dataset_uuid,
        dataset_lz_path_fun=get_dataset_lz_path,
        metadata_fun=read_metadata_file,
        include_file_metadata=False
    )

    def wrapped_send_status_msg(**kwargs):
        if send_status_msg(**kwargs):
            scanned_md = read_metadata_file(**kwargs)  # Yes, it's getting re-read
            kwargs['ti'].xcom_push(key='collectiontype',
                                   value=(scanned_md['collectiontype']
                                          if 'collectiontype' in scanned_md
                                          else None))
            if 'assay_type' in scanned_md:
                assay_type = scanned_md['assay_type']
            elif 'metadata' in scanned_md and 'assay_type' in scanned_md['metadata']:
                assay_type = scanned_md['metadata']['assay_type']
            else:
                assay_type = None
            kwargs['ti'].xcom_push(key='assay_type', value=assay_type)
        else:
            kwargs['ti'].xcom_push(key='collectiontype', value=None)

    t_send_status = PythonOperator(
        task_id='send_status_msg',
        python_callable=wrapped_send_status_msg,
        provide_context=True,
        trigger_rule='all_done',
        op_kwargs={
            'crypt_auth_tok': encrypt_tok(airflow_conf.as_dict()
                                          ['connections']['APP_CLIENT_SECRET']).decode(),
        }
    )

    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_temp_dir')

    t_check_uuids >> t_create_tmpdir >> t_run_md_extract >> t_md_consistency_tests >> t_send_status >> t_cleanup_tmpdir
