from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


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
