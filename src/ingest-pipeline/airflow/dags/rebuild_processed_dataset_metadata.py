from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

from utils import (
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    create_dataset_state_error_callback,
    get_tmp_dir_path,
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

with HMDAG('rebuild_processed_dataset_metadata',
           schedule_interval=None,
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('rebuild_metadata')
           }) as dag:

    # For now we just want to use the empty operator to test and make sure the rebuild multiple is working as expected.
    t_empty_operator = EmptyOperator(
        task_id='empty_operator'
    )

    t_empty_operator