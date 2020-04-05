import sys
import os
import json
import shlex
from pathlib import Path
from pprint import pprint
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.http_hook import HttpHook

import utils

sys.path.append(str(Path(__file__).resolve().parent.parent / 'lib'))
from schema_tools import assert_json_matches_schema

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
    'provide_context': True,
    'xcom_push': True,
    'queue': 'general'
}


def tmp_dir_path(run_id):
    return "{}/data/temp/{}".format(os.environ['AIRFLOW_HOME'], run_id)

with DAG('devtest_step2', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         max_active_runs=1,
         user_defined_macros={'tmp_dir_path' : tmp_dir_path}
         ) as dag:

    pipeline_name = 'devtest-step2-pipeline'

    def get_parent_dataset_uuid(**kwargs):
        return kwargs['dag_run'].conf['parent_submission_id']
    
    
    def get_dataset_uuid(**kwargs):
        return kwargs['ti'].xcom_pull(key='derived_dataset_uuid',
                                      task_ids="send_create_dataset")
    
    
    def build_dataset_name(**kwargs):
        return '{}__{}__{}'.format(dag.dag_id,
                                   kwargs['dag_run'].conf['parent_submission_id'],
                                   pipeline_name),


    t1 = PythonOperator(
        task_id='trigger_target',
        python_callable = utils.pythonop_trigger_target,
        )
    
    
    def build_cwltool_cmd1(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = tmp_dir_path(run_id)
        print('tmpdir: ', tmpdir)
        tmp_subdir = os.path.join(tmpdir, 'cwl_out')
        print('tmp_subdir: ', tmp_subdir)
        data_dir = ctx['parent_lz_path']
        print('data_dir: ', data_dir)

        try:
            delay_sec = int(ctx['metadata']['delay_sec'])
        except ValueError:
            print("Could not parse delay_sec "
                  "{} ; defaulting to 30 sec".format(ctx['metadata']['delay_sec']))
            delay_sec = 30
        for fname in ctx['metadata']['files_to_copy']:
            print(fname)

        command = [
            'sleep',
            '{}'.format(delay_sec),
            ';',
            'cd',
            data_dir,
            ';',
            'mkdir',
            '-p',
            '{}'.format(tmp_subdir),
            ';'
            ]
        
        if ctx['metadata']['files_to_copy']:
            command.extend(['cp'])
            command.extend(ctx['metadata']['files_to_copy'])
            command.extend([tmp_subdir])
        
        print('command list: ', command)
        command_str = ' '.join(piece if piece == ';' else shlex.quote(piece)
                               for piece in command)
        command_str = 'tmp_dir="{}" ; '.format(tmpdir) + command_str
        print('final command_str: %s' % command_str)
        return command_str


    t_build_cmd1 = PythonOperator(
        task_id='build_cmd1',
        python_callable=build_cwltool_cmd1
        )


    t_pipeline_exec = BashOperator(
        task_id='pipeline_exec',
        bash_command=""" \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )

    t_maybe_keep_cwl1 = BranchPythonOperator(
        task_id='maybe_keep_cwl1',
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'send_create_dataset',
                     'test_op' : 'pipeline_exec'}
        )

    t_no_keep = DummyOperator(
        task_id='no_keep')

    t_join = DummyOperator(
        task_id='join',
        trigger_rule='one_success')


    t_send_create_dataset = PythonOperator(
        task_id='send_create_dataset',
        python_callable=utils.pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs = {'parent_dataset_uuid_callable' : get_parent_dataset_uuid,
                     'http_conn_id' : 'ingest_api_connection',
                     'endpoint' : '/datasets/derived',
                     'dataset_name_callable' : build_dataset_name,
                     'dataset_types' :["dataset", "devtest"]
                     }
    )


    t_set_dataset_processing = PythonOperator(
        task_id='set_dataset_processing',
        python_callable=utils.pythonop_set_dataset_processing,
        provide_context=True,
        op_kwargs = {'dataset_uuid_callable' : get_dataset_uuid,
                     'http_conn_id' : 'ingest_api_connection',
                     'endpoint' : '/datasets/status'
                     }
    )


    t_move_data = BashOperator(
        task_id='move_data',
        bash_command="""
        tmp_dir="{{tmp_dir_path(run_id)}}" ; \
        ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
        groupname="{{conf.as_dict()['connections']['OUTPUT_GROUP_NAME']}}" ; \
        pushd "$ds_dir" ; \
        sudo chown airflow . ; \
        sudo chgrp $groupname . ; \
        popd ; \
        mv "$tmp_dir"/cwl_out/* "$ds_dir" >> "$tmp_dir/session.log" 2>&1 ; \
        echo $?
        """,
        provide_context=True
        )


    def send_status_msg(**kwargs):
        ctx = kwargs['dag_run'].conf
        retcode_ops = ['pipeline_exec', 'move_data']
        retcodes = [int(kwargs['ti'].xcom_pull(task_ids=op))
                    for op in retcode_ops]
        print('retcodes: ', {k:v for k, v in zip(retcode_ops, retcodes)})
        success = all([rc == 0 for rc in retcodes])
        derived_dataset_uuid = kwargs['ti'].xcom_pull(key='derived_dataset_uuid',
                                                      task_ids="send_create_dataset")
        ds_dir = kwargs['ti'].xcom_pull(task_ids='send_create_dataset')
        if 'metadata_to_return' in ctx['metadata']:
            md_to_return = ctx['metadata']['metadata_to_return']
        else:
            md_to_return = {}
        http_conn_id='ingest_api_connection'
        endpoint='/datasets/status'
        method='PUT'
        headers={
            'authorization' : 'Bearer ' + kwargs['dag_run'].conf['auth_tok'],
            'content-type' : 'application/json'}
        print('headers:')
        pprint(headers)
        extra_options=[]
         
        http = HttpHook(method,
                        http_conn_id=http_conn_id)
 
        if success:
            dag_prv = (kwargs['dag_run'].conf['dag_provenance']
                       if 'dag_provenance' in kwargs['dag_run'].conf
                       else {})
            pipeline_base_dir = os.path.join(os.environ['AIRFLOW_HOME'],
                                             'dags', 'cwl')
            dag_prv.update(utils.get_git_provenance_dict([__file__]))
            file_md = utils.get_file_metadata(ds_dir)
            md = {'dag_provenance' : dag_prv,
                  'files' : file_md,
                  'metadata' : md_to_return}
            try:
                assert_json_matches_schema(md, 'dataset_metadata_schema.yml')
                data = {'dataset_id' : derived_dataset_uuid,
                        'status' : 'QA',
                        'message' : 'the process ran',
                        'metadata': md}
            except AssertionError as e:
                print('invalid metadata follows:')
                pprint(md)
                data = {'dataset_id' : derived_dataset_uuid,
                        'status' : 'Error',
                        'message' : 'internal error; schema violation: {}'.format(e),
                        'metadata': {}}
        else:
            log_fname = os.path.join(tmp_dir_path(kwargs['run_id']),
                                     'session.log')
            with open(log_fname, 'r') as f:
                err_txt = '\n'.join(f.readlines())
            data = {'dataset_id' : derived_dataset_uuid,
                    'status' : 'Invalid',
                    'message' : err_txt}
        print('data: ')
        pprint(data)

        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)
        print('response: ')
        pprint(response.json())


    t_send_status = PythonOperator(
        task_id='send_status_msg',
        python_callable=send_status_msg,
        provide_context=True
    )

    
    t_create_tmpdir = BashOperator(
        task_id='create_temp_dir',
        bash_command='mkdir {{tmp_dir_path(run_id)}}',
        provide_context=True
        )


    t_cleanup_tmpdir = BashOperator(
        task_id='cleanup_temp_dir',
        bash_command='rm -r {{tmp_dir_path(run_id)}}',
        )
 

    (dag >> t1 >> t_create_tmpdir
     >> t_build_cmd1 >> t_pipeline_exec >> t_maybe_keep_cwl1
     >> t_send_create_dataset >> t_set_dataset_processing
     >> t_move_data
     >> t_send_status >> t_join)
    t_maybe_keep_cwl1 >> t_no_keep >> t_join
    t_join >> t_cleanup_tmpdir


