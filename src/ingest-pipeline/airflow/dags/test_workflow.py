from pprint import pprint
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

from utils import (
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    )

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
    'queue': get_queue_resource('test_workflow'),
}


with HMDAG('test_workflow',
           schedule_interval=None,
           is_paused_upon_creation=False,
           user_defined_macros={
               'tmp_dir_path': get_tmp_dir_path,
               'preserve_scratch': get_preserve_scratch_resource('test_workflow'),
           },
           default_args=default_args,
           ) as dag:

    def test_task_func(**kwargs):
        pprint(kwargs)

    t_test = PythonOperator(
        task_id='test_task',
        python_callable=test_task_func,
        provide_context=True,
        )

    t_create_tmpdir = CreateTmpDirOperator(task_id='create_tmp_dir')
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id='cleanup_tmp_dir')

    t_create_tmpdir >> t_test >> t_cleanup_tmpdir
