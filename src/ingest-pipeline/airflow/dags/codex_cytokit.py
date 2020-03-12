import sys
import os
from os import fspath
from pathlib import Path
import shlex
import yaml
import json
import pytz
from pprint import pprint
from datetime import datetime, timedelta
import glob

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.http_hook import HttpHook
from airflow.configuration import conf
from airflow.models import Variable

import utils

sys.path.append(str(Path(__file__).resolve().parent.parent / 'lib'))
from schema_tools import assert_json_matches_schema

import cwltool  # used to find its path

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

fake_conf = {'apply': 'codex_cytokit',
             'auth_tok': 'AgWob7mDM3vo141ny8WYXE47WJ0bWrelDJB5zd2qEr0MGqoGPKFgC38z2PO6r7r7WpNdNana8p0EBwf48YnE2Hq3bz',
             'component': '1_H3S1-1_L001_SI-GA-D8-1',
             'parent_lz_path': '/usr/local/airflow/lz/IEC Testing '
             'Group/cb8a37c188d2754f10dea76cd4958679',
             #'parent_lz_path': '/hubmap-data/test-stage/IEC Testing '
             #'Group/c7ebfd11223af5aa74ca42de211f87cd',
             'parent_submission_id': '80cd0a1fadbb654f023daf197d8a0bfe',
             'metadata': {'collectiontype': 'codex',
                          'components': ['1_H3S1-1_L001_SI-GA-D8-1',
                                         '1_H3S1-1_L002_SI-GA-D8-1'],
                          'dataset': {'10x_Genomics_Index_well_ID': 'SI-GA-D8',
                                      'Concentration_ng_ul': 16.8,
                                      'HuBMAP Case Identifier': 'Case3  Spleen  CC1',
                                      'Name': 'HBMP3_spleen_CC1',
                                      'Quantification_method': 'Qubit',
                                      'SI_INDEX_SEQUENCE': 'GCAACAAA',
                                      'Seq_run': 'NS-1699',
                                      'Sequencing_concentration_reported_from_ICBR': '225 pM',
                                      'UUID Identifier': 'UFL0001-SP-1-3',
                                      'Unnamed: 10': 'TAGTTGTC',
                                      'Unnamed: 11': 'CGCCATCG',
                                      'Unnamed: 12': 'ATTGGCGT',
                                      'Volume_ul': 10},
                          'tmc_uuid': 'raw_HBMP3_spleen_CC1'}}

with DAG('codex_cytokit', 
         schedule_interval=None, 
         is_paused_upon_creation=False, 
         default_args=default_args,
         max_active_runs=1
         ) as dag:

 
    def trigger_target(**kwargs):
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        print('run_id: ', run_id)
        print('dag_run.conf:')
        pprint(ctx)
        print('kwargs:')
        pprint(kwargs)
 

    t1 = PythonOperator(
        task_id='trigger_target',
        python_callable = trigger_target,
        )
    
    
    THREADS = 6
    pipeline_name = 'codex-pipeline'
    cwl_workflow1 = os.path.join(pipeline_name, 'pipeline.cwl')

#     prepare_cwl1 = PythonOperator(
#         python_callable=utils.clone_or_update_pipeline,
#         task_id='clone_or_update_cwl1',
#         op_kwargs={'pipeline_name': cwl_workflow1}
#     )

#     prepare_cwl2 = PythonOperator(
#         python_callable=utils.clone_or_update_pipeline,
#         task_id='clone_or_update_cwl2',
#         op_kwargs={'pipeline_name': cwl_workflow2}
#     )

    prepare_cwl1 = DummyOperator(
        task_id='prepare_cwl1'
        )
    
    prepare_cwl2 = DummyOperator(
        task_id='prepare_cwl2'
        )
    
    def build_cwltool_cmd1(**kwargs):
        #ctx = fake_conf
        ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = os.path.join(os.environ['AIRFLOW_HOME'],
                              'data', 'temp', run_id)
        print('tmpdir: ', tmpdir)
        data_dir = os.path.join(ctx['parent_lz_path'])
        print('data_dir: ', data_dir)
        pipeline_base_dir = str(Path(__file__).resolve().parent / 'cwl')
        cwltool_dir = os.path.dirname(cwltool.__file__)
        while cwltool_dir:
            part1, part2 = os.path.split(cwltool_dir)
            cwltool_dir = part1
            if part2 == 'lib':
                break
        assert cwltool_dir, 'Failed to find cwltool bin directory'
        cwltool_dir = os.path.join(cwltool_dir, 'bin')

        command = [
            'env',
            'PATH=%s:%s' % (cwltool_dir, os.environ['PATH']),
            'cwltool',
            os.path.join(pipeline_base_dir, cwl_workflow1),
            '--data_dir',
            data_dir,
        ]
        
#         command = [
#             'cp',
#             '-R',
#             os.path.join(os.environ['AIRFLOW_HOME'],
#                          'data', 'temp', 'std_salmon_out', 'cwl_out'),
#             tmpdir
#         ]
            
        command_str = ' '.join(shlex.quote(piece) for piece in command)
        print('final command_str: %s' % command_str)
        return command_str


    t_build_cmd1 = PythonOperator(
        task_id='build_cmd1',
        python_callable=build_cwltool_cmd1
        )


    t_pipeline_exec = BashOperator(
        task_id='pipeline_exec',
        queue='gpu000_q1',
        bash_command=""" \
        tmp_dir=${AIRFLOW_HOME}/data/temp/{{run_id}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        cd ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )

    def maybe_keep(**kwargs):
        """
        accepts the following via the caller's op_kwargs:
        'next_op': the operator to call on success
        'bail_op': the operator to which to bail on failure (default 'no_keep')
        'test_op': the operator providing the success code
        'test_key': xcom key to test.  Defaults to None for return code
        """
        bail_op = kwargs['bail_op'] if 'bail_op' in kwargs else 'no_keep'
        test_op = kwargs['test_op']
        test_key = kwargs['test_key'] if 'test_key' in kwargs else None
        retcode = int(kwargs['ti'].xcom_pull(task_ids=test_op, key=test_key))
        print('%s key %s: %s\n' % (test_op, test_key, retcode))
        if retcode is 0:
            return kwargs['next_op']
        else:
            return bail_op
    
    t_maybe_keep_cwl1 = BranchPythonOperator(
        task_id='maybe_keep_cwl1',
        python_callable=maybe_keep,
        provide_context=True,
        op_kwargs = {'next_op' : 'send_create_dataset',
                     'test_op' : 'pipeline_exec'}
        )

    t_no_keep = DummyOperator(
        task_id='no_keep')

    t_join = DummyOperator(
        task_id='join',
        trigger_rule='one_success')

    def send_create_dataset(**kwargs):
        #ctx = fake_conf
        ctx = kwargs['dag_run'].conf
        http_conn_id='ingest_api_connection'
        endpoint='/datasets/derived'
        method='POST'
        headers={
            'authorization' : 'Bearer ' + ctx['auth_tok'],
            'content-type' : 'application/json'}
        print('headers:')
        pprint(headers)
        extra_options=[]
        http = HttpHook(method,
                        http_conn_id=http_conn_id)
        data = {
            "source_dataset_uuid":ctx['parent_submission_id'],
            "derived_dataset_name":'{}__{}__{}'.format(dag.dag_id,
                                                       ctx['parent_submission_id'],
                                                       pipeline_name),
            "derived_dataset_types":["dataset",
                                     "codex",
                                     "cytokit"]
        }
        print('data: ')
        pprint(data)
        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)
        print('response: ')
        pprint(response.json())
        lz_root = os.path.split(ctx['parent_lz_path'])[0]
        lz_root = os.path.split(lz_root)[0]
        data_dir_path = os.path.join(lz_root,
                                     response.json()['group_display_name'],
                                     response.json()['derived_dataset_uuid'])
        kwargs['ti'].xcom_push(key='group_uuid',
                               value=response.json()['group_uuid'])
        kwargs['ti'].xcom_push(key='derived_dataset_uuid', 
                               value=response.json()['derived_dataset_uuid'])
        return data_dir_path

    t_send_create_dataset = PythonOperator(
        task_id='send_create_dataset',
        python_callable=send_create_dataset,
        provide_context=True
    )


    def set_dataset_processing(**kwargs):
        derived_dataset_uuid = kwargs['ti'].xcom_pull(key='derived_dataset_uuid',
                                                      task_ids="send_create_dataset")
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
 
        data = {'dataset_id' : derived_dataset_uuid,
                'status' : 'Processing',
                'message' : 'update state',
                'metadata': {}}
        print('data: ')
        pprint(data)

        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)
        print('response: ')
        pprint(response.json())


    t_set_dataset_processing = PythonOperator(
        task_id='set_dataset_processing',
        python_callable=set_dataset_processing,
        provide_context=True
    )


    t_move_data = BashOperator(
        task_id='move_data',
        bash_command="""
        tmp_dir="${AIRFLOW_HOME}/data/temp/{{run_id}}" ; \
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
        retcode_ops = ['pipeline_exec', 'move_data', 'make_arrow1', 'make_arrow2']
        retcodes = [int(kwargs['ti'].xcom_pull(task_ids=op))
                    for op in retcode_ops]
        print('retcodes: ', {k:v for k, v in zip(retcode_ops, retcodes)})
        success = all([rc == 0 for rc in retcodes])
        derived_dataset_uuid = kwargs['ti'].xcom_pull(key='derived_dataset_uuid',
                                                      task_ids="send_create_dataset")
        ds_dir = kwargs['ti'].xcom_pull(task_ids='send_create_dataset')
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
            dag_prv.update(utils.get_git_provenance_dict([__file__,
                                                          os.path.join(pipeline_base_dir,
                                                                       cwl_workflow1),
                                                          os.path.join(pipeline_base_dir,
                                                                       cwl_workflow2)]))
            file_md = utils.get_file_metadata(ds_dir)
            md = {'dag_provenance' : dag_prv,
                  'files' : file_md, 
                  'component': kwargs['dag_run'].conf['component']}
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
            log_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                     'data/temp', kwargs['run_id'],
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
        bash_command='mkdir ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        provide_context=True
        )


    t_cleanup_tmpdir = BashOperator(
        task_id='cleanup_temp_dir',
        bash_command='echo rm -r ${AIRFLOW_HOME}/data/temp/{{run_id}}',
        )
 

    (dag >> t1 >> t_create_tmpdir
     >> prepare_cwl1 # >> prepare_cwl2
     >> t_build_cmd1 >> t_pipeline_exec >> t_maybe_keep_cwl1
     >> t_send_create_dataset >> t_set_dataset_processing
     >> t_move_data
     >> t_send_status >> t_join)
    t_maybe_keep_cwl1 >> t_no_keep >> t_join
    t_join >> t_cleanup_tmpdir


