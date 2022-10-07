from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator as AWSBatchOperator

import utils

from utils import (
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
)


def get_uuid_for_error(**kwargs):
    """
    Return the uuid for the derived dataset if it exists, and of the parent dataset otherwise.
    """
    return None


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
    'queue': get_queue_resource('test_aws_batch'),
    'on_failure_callback': utils.create_dataset_state_error_callback(get_uuid_for_error)
}


with HMDAG('test_aws_batch', 
           schedule_interval=None, 
           is_paused_upon_creation=False, 
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path' : utils.get_tmp_dir_path,
               'preserve_scratch' : get_preserve_scratch_resource('test_aws_batch'),
           }) as dag:

    my_conn_id = 'aws_default'
    my_overrides = {}  # vcpus, memory, command, instanceType, environment, resourceRequirements
    my_useless = {
        'region_name': 'us-east-1',
        'signature_version': 'v4',
        'retries': {'max_attempts': 10, 'mode': 'standard'}
    }
    my_params = {}
    batch_task = AWSBatchOperator(task_id='aws_batch_task',
                                  job_name='test-airflow-submission', 
                                  job_definition='test-job-definition',
                                  job_queue='test-queue', 
                                  overrides=my_overrides,
                                  parameters=my_params,
                                  aws_conn_id=my_conn_id)

    batch_task
