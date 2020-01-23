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
             'auth_tok': 'AgJEbW7KYQVDvpdWJ9vJWGDrYa0W0zp5vJ3e1vP3mz8ydzOjO6T0CBxzxEnKmJeOedyx58j44EDDM7U395N0Yf7KVD',
             'component': '1_H3S1-3_L001_SI-GA-D8-3',
             'parent_lz_path': '/usr/local/airflow/lz/IEC Testing '
             'Group/80cd0a1fadbb654f023daf197d8a0bfe',
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
        #ctx = dag_run.conf
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

        command = [
            'echo',
            '{"message":"hello world"}',
            ">",
            os.path.join(tmpdir,'meta.json')]

#         command = [
#             'env',
#             'PATH=%s:$PATH' % cwltool_dir,
#             'cwltool',
#             '--outdir',
#             tmpdir,
#             '--basedir',
#             tmpdir,
#             '--parallel',
#             os.path.join(pipeline_base_dir, pipeline_name, 'pipeline.cwl'),
#             '--fastq_r1',
#             fastq_r1,
#             '--fastq_r2',
#             fastq_r2,
#             '--threads',
#             str(THREADS),
#         ]
        
        command_str = ' '.join(shlex.quote(piece) for piece in command)
        print('final command_str: %s' % command_str)
        return command_str

    build_cmd = PythonOperator(
        task_id='build_cmd',
        python_callable=build_cwltool_cmd
        )

    pipeline_exec = BashOperator(
        task_id='pipeline_exec',
        bash_command="{{ti.xcom_pull(task_ids='build_cmd')}}",
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
     >> t_cleanup_tmpdir
     )



# Hardcoded parameters for first Airflow execution
DATA_DIRECTORY = Path('/hive/hubmap/data/CMU_Tools_Testing_Group/salmon-rnaseq')
FASTQ_R1 = DATA_DIRECTORY / 'L001_R1_001_r.fastq.gz'
FASTQ_R2 = DATA_DIRECTORY / 'L001_R2_001_r.fastq.gz'

