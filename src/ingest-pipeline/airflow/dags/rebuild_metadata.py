import os
import yaml

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils import (
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    create_dataset_state_error_callback,
    pythonop_md_consistency_tests,
)

from hubmap_operators.common_operators import (
    make_send_status_msg_function,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
    get_tmp_dir_path,
)


def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    return None


def get_dataset_uuid(**kwargs):
    ctx = kwargs['dag_run'].conf
    return ctx['submission_id']


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
    'queue': get_queue_resource('launch_multi_analysis'),
    'on_failure_callback': create_dataset_state_error_callback(get_uuid_for_error)
}

with HMDAG('rebuild_metadata',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('launch_multi_analysis')
           }) as dag:

    t_create_tmpdir = CreateTmpDirOperator(task_id='create_temp_dir')

    t_run_md_extract = BashOperator(
        task_id='run_md_extract',
        bash_command=""" \
            lz_dir="{{dag_run.conf.lz_path}}" ; \
            src_dir="{{dag_run.conf.src_path}}/md" ; \
            top_dir="{{dag_run.conf.src_path}}" ; \
            work_dir="{{tmp_dir_path(run_id)}}" ; \
            cd $work_dir ; \
            env PYTHONPATH=${PYTHONPATH}:$top_dir \
            python $src_dir/metadata_extract.py --out ./rslt.yml --yaml "$lz_dir" \
              >> session.log 2> error.log ; \
            echo $? ; \
            if [ -s error.log ] ; \
            then echo 'ERROR!' `cat error.log` >> session.log ; \
            else rm error.log ; \
            fi
            """
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
        retcode_ops=['run_validation', 'run_md_extract', 'md_consistency_tests'],
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
        trigger_rule='all_done'
    )

    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_temp_dir')

    t_create_tmpdir >> t_run_md_extract >> t_md_consistency_tests >> t_send_status >> t_cleanup_tmpdir
