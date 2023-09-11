from pprint import pprint
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from utils import (
    HMDAG,
    get_queue_resource,
    get_auth_tok,
    )

with add_path(airflow_conf.as_dict()["connections"]["SRC_PATH"].strip("'").strip('"')):
    from submodules import hubmapbags

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
    'queue': get_queue_resource('generate_today_json'),
}


with HMDAG('generate_today_json',
           schedule_interval="0 0 * * *",
           is_paused_upon_creation=False,
           default_args=default_args,
           user_defined_macros={
           }) as dag:

    def generate_report():
        token = get_auth_tok()
        hubmapbags.utilities.clean()
        hubmapbags.reports.daily(token)

    t_generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report(),
        provide_context=True,
        )

    t_generate_report()