import sys
import os
from pprint import pprint

import yaml
from pathlib import Path
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.ec2 import EC2StartInstanceOperator, EC2StopInstanceOperator
from airflow.providers.amazon.aws.sensors.ec2 import EC2InstanceStateSensor

from airflow.configuration import conf as airflow_conf
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.exceptions import AirflowException

from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator
from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

import utils

from utils import (
    make_send_status_msg_function,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    pythonop_maybe_keep,
    pythonop_get_dataset_state,
    )

sys.path.append(airflow_conf.as_dict()['connections']['SRC_PATH']
                .strip("'").strip('"'))
from submodules import (ingest_validation_tools_upload,  # noqa E402
                        ingest_validation_tools_error_report,
                        ingest_validation_tests)
sys.path.pop()


def get_dataset_uuid(**kwargs):
    ctx = kwargs['dag_run'].conf
    return ctx['submission_id']


def get_dataset_lz_path(**kwargs):
    ctx = kwargs['dag_run'].conf
    return ctx['lz_path']


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
    'queue': get_queue_resource('scan_and_begin_processing'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_dataset_uuid)    
}


with HMDAG('scan_and_begin_processing', 
           schedule_interval=None, 
           is_paused_upon_creation=False, 
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': utils.get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('scan_and_begin_processing')
           }) as dag:

    t_start_instance = EC2StartInstanceOperator(
        task_id='start_instance',
        instance_id='i-007bbde390bf07819',
        region_name='us-east-1'
    )

    t_sense_start_instance = EC2InstanceStateSensor(
        task_id='await_start_instance',
        instance_id='i-007bbde390bf07819',
        target_state='running'
    )

    def read_metadata_file(**kwargs):
        md_fname = os.path.join(utils.get_tmp_dir_path(kwargs['run_id']),
                                'rslt.yml')
        with open(md_fname, 'r') as f:
            scanned_md = yaml.safe_load(f)
        return scanned_md

    
    def run_validation(**kwargs):
        if ('lz_path' in kwargs['dag_run'].conf
            and 'submission_id' in kwargs['dag_run'].conf):
            # These conditions are set by the hubap_api plugin when this DAG
            # is invoked from the ingest user interface
            lz_path = kwargs['dag_run'].conf['lz_path']
            uuid = kwargs['dag_run'].conf['submission_id']
        elif 'parent_submission_id' in kwargs['dag_run'].conf:
            # These conditions are met when this DAG is triggered via
            # the launch_multi_analysis DAG.
            uuid_list = kwargs['dag_run'].conf['parent_submission_id']
            assert len(uuid_list) == 1, f"{dag.dag_id} can only handle one uuid at a time"
            def my_callable(**kwargs):
                return uuid_list[0]
            ds_rslt = pythonop_get_dataset_state(
                dataset_uuid_callable=my_callable,
                **kwargs
            )
            if not ds_rslt:
                raise AirflowException(f'Invalid uuid/doi for group: {uuid}')
            if not 'local_directory_full_path' in ds_rslt:
                raise AirflowException(f'Dataset status for {uuid_list[0]} has no full path')
            lz_path = ds_rslt['local_directory_full_path']
            uuid = uuid_list[0]  # possibly translating a HuBMAP ID
        else:
            raise AirflowException("The dag_run does not contain enough information")
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
            add_notes=False,
            ignore_deprecation=True
        )
        # Scan reports an error result
        errors = upload.get_errors(plugin_kwargs=kwargs)
        if errors:
            info = upload.get_info()
            report = ingest_validation_tools_error_report.ErrorReport(errors=errors,
                                                                      info=info)
            sys.stdout.write('Directory validation failed! Errors follow:\n')
            sys.stdout.write(report.as_text())
            log_fname = os.path.join(utils.get_tmp_dir_path(kwargs['run_id']),
                                     'session.log')
            with open(log_fname, 'w') as f:
                f.write('Directory validation failed! Errors follow:\n')
                f.write(report.as_text())
            return 1
        else:
            return 0

    t_run_validation = PythonOperator(
        task_id='run_validation',
        python_callable=run_validation,
        provide_context=True,
        op_kwargs={
        }
    )

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

    t_maybe_continue = BranchPythonOperator(
        task_id='maybe_continue',
        python_callable=pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            'next_op': 'run_md_extract',
            'bail_op': 'send_status_msg',
            'test_op': 'run_validation',
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
        python_callable=utils.pythonop_md_consistency_tests,
        provide_context=True,
        op_kwargs={'metadata_fname': 'rslt.yml'}
        )

    t_send_status = PythonOperator(
        task_id='send_status_msg',
        python_callable=wrapped_send_status_msg,
        provide_context=True,
        trigger_rule='all_done'
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id='create_temp_dir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_temp_dir')

    def flex_maybe_spawn(**kwargs):
        """
        This is a generator which returns appropriate DagRunOrders
        """
        print('kwargs:')
        pprint(kwargs)
        print('dag_run conf:')
        ctx = kwargs['dag_run'].conf
        pprint(ctx)
        run_validation_retcode = int(kwargs['ti'].xcom_pull(task_ids="run_validation"))
        md_extract_retcode = kwargs['ti'].xcom_pull(task_ids="run_md_extract")
        md_extract_retcode = int(md_extract_retcode or '0')
        md_consistency_retcode = kwargs['ti'].xcom_pull(task_ids="md_consistency_tests")
        md_consistency_retcode = int(md_consistency_retcode or '0')
        if run_validation_retcode == 0 and md_extract_retcode == 0 and md_consistency_retcode == 0:
            collectiontype = kwargs['ti'].xcom_pull(key='collectiontype',
                                                    task_ids="send_status_msg")
            assay_type = kwargs['ti'].xcom_pull(key='assay_type',
                                                task_ids="send_status_msg")
            print('collectiontype: <{}>, assay_type: <{}>'.format(collectiontype, assay_type))
            md_fname = os.path.join(utils.get_tmp_dir_path(kwargs['run_id']), 'rslt.yml')
            with open(md_fname, 'r') as f:
                md = yaml.safe_load(f)
            # payload = {k:kwargs['dag_run'].conf[k] for k in kwargs['dag_run'].conf}
            payload = {'ingest_id': ctx['run_id'],
                       'crypt_auth_tok': ctx['crypt_auth_tok'],
                       'parent_lz_path': ctx['lz_path'],
                       'parent_submission_id': ctx['submission_id'],
                       'metadata': md,
                       'dag_provenance_list': utils.get_git_provenance_list(__file__)
                       }
            for next_dag in utils.downstream_workflow_iter(collectiontype, assay_type):
                yield next_dag, payload
        else:
            return None


    t_maybe_spawn = FlexMultiDagRunOperator(
        task_id='flex_maybe_spawn',
        provide_context=True,
        python_callable=flex_maybe_spawn
        )

    t_stop_instance = EC2StopInstanceOperator(
        task_id='stop_instance',
        instance_id='i-007bbde390bf07819',
        region_name='us-east-1'
    )

    t_sense_stop_instance = EC2InstanceStateSensor(
        task_id='await_stop_instance',
        instance_id='i-007bbde390bf07819',
        target_state='stopped'
    )

    (
        t_start_instance
        >> t_sense_start_instance
        >> t_create_tmpdir
        >> t_run_validation
        >> t_maybe_continue
        >> t_run_md_extract
        >> t_md_consistency_tests
        >> t_send_status
        >> t_maybe_spawn
        >> t_cleanup_tmpdir
        >> t_stop_instance
        >> t_sense_stop_instance
     )

    t_maybe_continue >> t_send_status
    t_send_status >> t_stop_instance
    t_stop_instance >> t_sense_stop_instance
