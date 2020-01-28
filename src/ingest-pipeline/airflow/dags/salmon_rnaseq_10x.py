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

from utils import PIPELINE_BASE_DIR, clone_or_update_pipeline

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
}

fake_conf = {'apply': 'salmon_rnaseq_10x',
             'auth_tok': 'Agqm11doB0qVzQQrvKmV8xQxprd2P3lamNeMO5prJOQnrmJN7ghqCPq4QrV4OWn36eV62PGMG14we7IP4dovVUlPrY',
             'component': '1_H3S1-3_L001_SI-GA-D8-3',
             #'parent_lz_path': '/usr/local/airflow/lz/IEC Testing '
             #'Group/80cd0a1fadbb654f023daf197d8a0bfe',
             'parent_lz_path': '/hubmap-data/test-stage/IEC Testing '
             'Group/c7ebfd11223af5aa74ca42de211f87cd',
             'parent_submission_id': '80cd0a1fadbb654f023daf197d8a0bfe',
             'metadata': {'collectiontype': 'rnaseq_10x',
                          'components': ['1_H3S1-1_L001_SI-GA-D8-1',
                                         '1_H3S1-1_L002_SI-GA-D8-1',
                                         '1_H3S1-1_L003_SI-GA-D8-1',
                                         '1_H3S1-2_L001_SI-GA-D8-2',
                                         '1_H3S1-2_L002_SI-GA-D8-2',
                                         '1_H3S1-2_L003_SI-GA-D8-2',
                                         '1_H3S1-3_L001_SI-GA-D8-3',
                                         '1_H3S1-3_L002_SI-GA-D8-3',
                                         '1_H3S1-3_L003_SI-GA-D8-3',
                                         '1_H3S1-4_L001_SI-GA-D8-4',
                                         '1_H3S1-4_L002_SI-GA-D8-4',
                                         '1_H3S1-4_L003_SI-GA-D8-4'],
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

with DAG('salmon_rnaseq_10x', 
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
    pipeline_name = 'salmon_rnaseq_10x'

#     prepare_pipeline = PythonOperator(
#         python_callable=clone_or_update_pipeline,
#         task_id='clone_or_update_pipeline',
#         op_kwargs={'pipeline_name': pipeline_name}
#     )

    prepare_pipeline = DummyOperator(
        task_id='prepare_pipeline'
        )
    
    def build_cwltool_cmd(**kwargs):
        ctx = fake_conf
        #ctx = kwargs['dag_run'].conf
        run_id = kwargs['run_id']
        tmpdir = os.path.join(os.environ['AIRFLOW_HOME'],
                              'data', 'temp', run_id)
        print('tmpdir: ', tmpdir)
        datadir = os.path.join(ctx['parent_lz_path'],
                               ctx['metadata']['tmc_uuid'],
                               ctx['component'])
        print('datadir: ', datadir)
        r1_candidates = glob.glob(os.path.join(datadir, '*_R1_???.fastq.gz'))
        assert len(r1_candidates) == 1, 'zero or too many R1 fastq files'
        fastq_r1 = r1_candidates[0]
        r2_candidates = glob.glob(os.path.join(datadir, '*_R2_???.fastq.gz'))
        assert len(r2_candidates) == 1, 'zero or too many R1 fastq files'
        fastq_r2 = r2_candidates[0]
        pipeline_base_dir = os.path.join(os.environ['AIRFLOW_HOME'],
                                         'dags', 'cwl')
        cwltool_dir = os.path.dirname(cwltool.__file__)
        while cwltool_dir:
            part1, part2 = os.path.split(cwltool_dir)
            cwltool_dir = part1
            if part2 == 'lib':
                break
        assert cwltool_dir, 'Failed to find cwltool bin directory'
        cwltool_dir = os.path.join(cwltool_dir, 'bin')

#         # Avoid cwltool problems while debugging
#         command = [
#             'echo',
#             'hello world'
#             ]
        # make some pretend output
        with open(os.path.join(tmpdir, 'meta.yml'), 'w') as f:
            yaml.dump({'message':'hello world'}, f)

        command = [
            'env',
            'PATH=%s:%s' % (cwltool_dir, os.environ['PATH']),
            'cwltool',
            '--debug',
            '--outdir',
            os.path.join(tmpdir, 'cwl_out'),
#             '--basedir',
#             os.path.join(tmpdir, 'cwl_base'),
            '--parallel',
            os.path.join(pipeline_base_dir, pipeline_name, 'pipeline.cwl'),
            '--fastq_r1',
            fastq_r1,
            '--fastq_r2',
            fastq_r2,
            '--threads',
            str(THREADS),
        ]
        
        command_str = ' '.join(shlex.quote(piece) for piece in command)
        print('final command_str: %s' % command_str)
        return command_str

    build_cmd = PythonOperator(
        task_id='build_cmd',
        python_callable=build_cwltool_cmd
        )

    pipeline_exec = BashOperator(
        task_id='pipeline_exec',
        bash_command=""" \
        tmp_dir=${AIRFLOW_HOME}/data/temp/{{run_id}} ; \
        {{ti.xcom_pull(task_ids='build_cmd')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """
    )

    def maybe_keep(**kwargs):
        cwl_retcode = int(kwargs['ti'].xcom_pull(task_ids="pipeline_exec"))
        print('cwl_retcode: ', cwl_retcode)
        if cwl_retcode is 0:
            return 'send_create_dataset'
        else:
            return 'no_keep'
    
    t_maybe_keep = BranchPythonOperator(
        task_id='maybe_keep',
        python_callable=maybe_keep,
        provide_context=True
        )

    t_no_keep = DummyOperator(
        task_id='no_keep')

    t_join = DummyOperator(
        task_id='join',
        trigger_rule='one_success')

    def send_create_dataset(**kwargs):
        ctx = fake_conf
        #ctx = kwargs['dag_run'].conf
        http_conn_id='ingest_api_connection'
        endpoint='/datasets/derived'
        method='POST'
        headers={
            'authorization' : 'Bearer ' + ctx['auth_tok'],
            'content-type' : 'application/json'}
        extra_options=[]
        http = HttpHook(method,
                        http_conn_id=http_conn_id)
        data = {
            "source_dataset_uuid":ctx['parent_submission_id'],
            "derived_dataset_name":'{}__{}__{}'.format(ctx['metadata']['tmc_uuid'],
                                                       ctx['component'],
                                                       pipeline_name),
            "derived_dataset_entity_type":"Dataset"
        }
        print('data: ', data)
        response = http.run(endpoint,
                            json.dumps(data),
                            headers,
                            extra_options)
        print('response: ', response.text)
        lz_root = os.path.split(ctx['parent_lz_path'])[0]
        lz_root = os.path.split(lz_root)[0]
        data_dir_path = os.path.join(lz_root,
                                     response.json()['group_display_name'],
                                     response.json()['derived_dataset_uuid'])
        kwargs['ti'].xcom_push(key='group_uuid', value=response.json()['group_uuid'])
        return data_dir_path

    t_send_create_dataset = PythonOperator(
        task_id='send_create_dataset',
        python_callable=send_create_dataset,
        provide_context=True
    )

    t_move_data = BashOperator(
        task_id='move_data',
        bash_command="""
        tmp_dir="${AIRFLOW_HOME}/data/temp/{{run_id}}" ; \
        ds_dir="{{ti.xcom_pull(task_ids="send_create_dataset")}}" ; \
        grp_uuid="{{ti.xcom_pull(key="group_uuid", task_ids="send_create_dataset")}}" ; \
        mkdir "$ds_dir/$grp_uuid" >> "$tmp_dir/session.log" 2>&1; \
        mv "$tmp_dir"/* "$ds_dir/$grp_uuid" >> "$tmp_dir/session.log" 2>&1 ; \
        echo $?
        """,
        provide_context=True
        )

    def send_status_msg(**kwargs):
        cwl_retcode = int(kwargs['ti'].xcom_pull(task_ids="pipeline_exec"))
        print('cwl_retcode: ', cwl_retcode)
        cp_retcode =  int(kwargs['ti'].xcom_pull(task_ids="move_data"))
        print('cp_retcode: ', cp_retcode)
        http_conn_id='ingest_api_connection'
        endpoint='/datasets/status'
        method='POST'
        headers={
            'authorization' : 'Bearer ' + kwargs['dag_run'].conf['auth_tok'],
            'content-type' : 'application/json'}
        extra_options=[]
         
        http = HttpHook(method,
                        http_conn_id=http_conn_id)
 
        if cwl_retcode == 0 and cp_retcode == 0:
            md_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                    'data/temp', kwargs['run_id'],
                                    'meta.yml')
            with open(md_fname, 'r') as f:
                md = yaml.safe_load(f)
            data = {'ingest_id' : kwargs['dag_run'].conf['ingest_id'],
                    'status' : 'success',
                    'message' : 'the process ran',
                    'metadata': md}
        else:
            log_fname = os.path.join(os.environ['AIRFLOW_HOME'],
                                     'data/temp', kwargs['run_id'],
                                     'session.log')
            with open(log_fname, 'r') as f:
                err_txt = '\n'.join(f.readlines())
            data = {'ingest_id' : kwargs['dag_run'].conf['ingest_id'],
                    'status' : 'failure',
                    'message' : err_txt}
        print('data: ', data)

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
     >> prepare_pipeline >> build_cmd >> pipeline_exec
     >> t_maybe_keep)
    t_maybe_keep >> t_send_create_dataset >> t_move_data >> t_send_status >> t_join
    t_maybe_keep >> t_no_keep >> t_join
    t_join >> t_cleanup_tmpdir



# # Hardcoded parameters for first Airflow execution
# DATA_DIRECTORY = Path('/hive/hubmap/data/CMU_Tools_Testing_Group/salmon-rnaseq')
# FASTQ_R1 = DATA_DIRECTORY / 'L001_R1_001_r.fastq.gz'
# FASTQ_R2 = DATA_DIRECTORY / 'L001_R2_001_r.fastq.gz'

