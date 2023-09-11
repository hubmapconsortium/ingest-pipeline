import sys
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.configuration import conf as airflow_conf

from utils import (
    HMDAG,
    get_queue_resource,
    get_auth_tok,
    encrypt_tok,
    )

class add_path:
    """
    Add an element to sys.path using a context.
    Thanks to Eugene Yarmash https://stackoverflow.com/a/39855753
    """

    def __init__(self, path):
        self.path = path

    def __enter__(self):
        sys.path.insert(0, self.path)

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            sys.path.remove(self.path)
        except ValueError:
            pass

with add_path(airflow_conf.as_dict()["connections"]["SRC_PATH"].strip("'").strip('"')):
    from submodules import hubmapbags

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime.date.today(),
    'email': ['joel.welling@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'xcom_push': True,
    'queue': get_queue_resource('generate_today_json'),
}


with HMDAG('generate_today_json',
           schedule_interval="0 0 * * *",
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
           }) as dag:

    def generate_report(**kwargs):
        token = get_auth_tok(**kwargs)
        hubmapbags.utilities.clean()
        hubmapbags.reports.daily(token)

    t_generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            ),
        },
        )

    t_generate_report