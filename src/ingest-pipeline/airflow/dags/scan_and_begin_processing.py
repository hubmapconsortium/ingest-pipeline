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

from utils import localized_assert_json_matches_schema as assert_json_matches_schema


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
        
    def send_status_msg(**kwargs):
        ctx = kwargs['dag_run'].conf
        retcode_ops = ['run_md_extract', 'md_consistency_tests']
        print('raw: ', [kwargs['ti'].xcom_pull(task_ids=op) for op in retcode_ops])
        retcodes = [int(kwargs['ti'].xcom_pull(task_ids=op))
                    for op in retcode_ops]
        retcode_dct = {k:v for k, v in zip(retcode_ops, retcodes)}
        print('retcodes: ', retcode_dct)
        success = all([rc == 0 for rc in retcodes])
        ds_dir = ctx['lz_path']
        http_conn_id='ingest_api_connection'
        endpoint='/datasets/status'
        method='PUT'
        headers={'authorization' : 'Bearer ' + utils.decrypt_tok(ctx['crypt_auth_tok'].encode()),
                 'content-type' : 'application/json'}
        print('headers:')
        pprint(headers)
        extra_options=[]
         
        http = HttpHook(method,
                        http_conn_id=http_conn_id)
 
        if success:
            md_fname = os.path.join(utils.get_tmp_dir_path(kwargs['run_id']),
                                    'rslt.yml')
            with open(md_fname, 'r') as f:
                scanned_md = yaml.safe_load(f)
            dag_prv = utils.get_git_provenance_list([__file__])
            md = {'dag_provenance_list' : dag_prv,
                  'metadata' : scanned_md}
            # Inclusion of files information in this message is getting disabled due to size
            #md.update(utils.get_file_metadata_dict(ds_dir,
            #                                       utils.get_tmp_dir_path(kwargs['run_id'])))
            try:
                assert_json_matches_schema(md, 'dataset_metadata_schema.yml')
                data = {'dataset_id' : ctx['submission_id'],
                        'status' : 'QA',
                        'message' : 'the process ran',
                        'metadata': md}
            except AssertionError as e:
                print('invalid metadata follows:')
                pprint(md)
                data = {'dataset_id' : ctx['submission_id'],
                        'status' : 'Error',
                        'message' : 'internal error; schema violation: {}'.format(e),
                        'metadata': {}}
            kwargs['ti'].xcom_push(key='collectiontype',
                                   value=(scanned_md['collectiontype']
                                          if 'collectiontype' in scanned_md
                                          else None))
            kwargs['ti'].xcom_push(key='assay_type',
                                   value=(scanned_md['assay_type']
                                          if 'assay_type' in scanned_md
                                          else None))
        else:
            for op in retcode_ops:
                if retcode_dct[op]:
                    if op == 'run_md_extract':
                        log_fname = os.path.join(utils.get_tmp_dir_path(kwargs['run_id']),
                                                 'session.log')
                        with open(log_fname, 'r') as f:
                            err_txt = '\n'.join(f.readlines())
                    else:
                        err_txt = kwargs['ti'].xcom_pull(task_ids=op, key='err_msg')
                    break
            else:
                err_txt = 'Unknown error'
            data = {'dataset_id' : ctx['submission_id'],
                    'status' : 'Invalid',
                    'message' : err_txt}
            kwargs['ti'].xcom_push(key='collectiontype', value=None)
        print('data: ')
        pprint(data)
         
        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)
        print('response: ')
        pprint(response.json())

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
          > ./session.log 2>&1 ; \
        echo $?
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
        python_callable=send_status_msg,
        provide_context=True
        )

    t_create_tmpdir = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir {{tmp_dir_path(run_id)}}'
        )


    t_cleanup_tmpdir = BashOperator(
        task_id='cleanup_temp_dir',
        bash_command='echo rm -r {{tmp_dir_path(run_id)}}',
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

