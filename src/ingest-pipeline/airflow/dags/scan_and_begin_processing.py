import sys
import os
import yaml
import json
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator, DagRunOrder
from airflow.operators.multi_dagrun import TriggerMultiDagRunOperator
from airflow.hooks.http_hook import HttpHook

from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator
import utils

from utils import (
    localized_assert_json_matches_schema as assert_json_matches_schema,
    make_send_status_msg_function
    )


def get_dataset_uuid(**kwargs):
    ctx = kwargs['dag_run'].conf
    return ctx['submission_id']


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
    'queue': utils.map_queue_name('general'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_dataset_uuid)    
}


with DAG('scan_and_begin_processing', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         user_defined_macros={'tmp_dir_path' : utils.get_tmp_dir_path}         
         ) as dag:

    def read_metadata_file(**kwargs):
        md_fname = os.path.join(utils.get_tmp_dir_path(kwargs['run_id']),
                                'rslt.yml')
        with open(md_fname, 'r') as f:
            scanned_md = yaml.safe_load(f)
        return scanned_md

    def get_blank_dataset_lz_path(**kwargs):
        return ''  # used to suppress sending of file metadata

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=['run_md_extract', 'md_consistency_tests'],
        cwl_workflows=[],
        dataset_uuid_fun=get_dataset_uuid,
        dataset_lz_path_fun=get_blank_dataset_lz_path,
        metadata_fun=read_metadata_file
    )

    def wrapped_send_status_msg(**kwargs):
        if send_status_msg(**kwargs):
            scanned_md = read_metadata_file(**kwargs) # Yes, it's getting re-read
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
          > session.log 2> error.log ; \
        echo $? ; \
        if [ -s error.log ] ; \
        then echo 'ERROR!' `cat error.log` >> session.log ; \
        else rm error.log ; \
        fi
        """
        )

    t_md_consistency_tests = PythonOperator(
        task_id='md_consistency_tests',
        python_callable=utils.pythonop_md_consistency_tests,
        provide_context=True,
        op_kwargs = {'metadata_fname' : 'rslt.yml'}
        )

    t_send_status = PythonOperator(
        task_id='send_status_msg',
        python_callable=wrapped_send_status_msg,
        provide_context=True
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


    def flex_maybe_spawn(**kwargs):
        """
        This is a generator which returns appropriate DagRunOrders
        """
        print('kwargs:')
        pprint(kwargs)
        print('dag_run conf:')
        ctx = kwargs['dag_run'].conf
        pprint(ctx)
        md_extract_retcode = int(kwargs['ti'].xcom_pull(task_ids="run_md_extract"))
        md_consistency_retcode = int(kwargs['ti'].xcom_pull(task_ids="md_consistency_tests"))
        if md_extract_retcode == 0 and md_consistency_retcode == 0:
            collectiontype = kwargs['ti'].xcom_pull(key='collectiontype',
                                                    task_ids="send_status_msg")
            assay_type = kwargs['ti'].xcom_pull(key='assay_type',
                                                task_ids="send_status_msg")
            print('collectiontype: <{}>, assay_type: <{}>'.format(collectiontype, assay_type))
            md_fname = os.path.join(utils.get_tmp_dir_path(kwargs['run_id']), 'rslt.yml')
            with open(md_fname, 'r') as f:
                md = yaml.safe_load(f)
            payload = {k:kwargs['dag_run'].conf[k] for k in kwargs['dag_run'].conf}
            payload = {'ingest_id' : ctx['run_id'],
                       'crypt_auth_tok' : ctx['crypt_auth_tok'],
                       'parent_lz_path' : ctx['lz_path'],
                       'parent_submission_id' : ctx['submission_id'],
                       'metadata': md,
                       'dag_provenance_list' : utils.get_git_provenance_list(__file__)
                       }
            for next_dag in utils.downstream_workflow_iter(collectiontype, assay_type):
                yield next_dag, DagRunOrder(payload=payload)
        else:
            return None


    t_maybe_spawn = FlexMultiDagRunOperator(
        task_id='flex_maybe_spawn',
        provide_context=True,
        python_callable=flex_maybe_spawn
        )

    (dag >> t_create_tmpdir >> t_run_md_extract >> t_md_consistency_tests >>
     t_send_status >> t_maybe_spawn >> t_cleanup_tmpdir)

