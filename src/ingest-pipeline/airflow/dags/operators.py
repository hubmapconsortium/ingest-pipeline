from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from utils import (
    get_dataset_uuid,
    pythonop_set_dataset_state,
    pythonop_trigger_target,
)


t1 = PythonOperator(
    task_id='trigger_target',
    python_callable=pythonop_trigger_target,
)

t_join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
)

t_create_tmpdir = BashOperator(
    task_id='create_temp_dir',
    bash_command='mkdir {{tmp_dir_path(run_id)}}',
    provide_context=True,
)

t_cleanup_tmpdir = BashOperator(
    task_id='cleanup_temp_dir',
    bash_command='rm -r {{tmp_dir_path(run_id)}}',
    trigger_rule='all_success',
)

t_set_dataset_processing = PythonOperator(
    task_id='set_dataset_processing',
    python_callable=pythonop_set_dataset_state,
    provide_context=True,
    op_kwargs={
        'dataset_uuid_callable': get_dataset_uuid,
        'http_conn_id': 'ingest_api_connection',
        'endpoint': '/datasets/status',
    },
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
    provide_context=True,
)
