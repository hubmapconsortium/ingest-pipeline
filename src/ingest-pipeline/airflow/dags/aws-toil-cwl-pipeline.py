from airflow import DAG, models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hubmap',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email': ['jpuerto@psc.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('globus_transfer', schedule_interval=None, is_paused_upon_creation=False, default_args=default_args) as dag:
    def build_cwltool_cmd(**kwargs):
        ctx = kwargs['dag_run'].conf
        command_str = 'toil-cwl-runner --provisioner aws --jobStore aws:us-west-2:toil-cluster /root/cwl_workflows/' + ctx['repository_name'] + '/' + ctx['workflow_file'] + ' ' + ctx['cli_args']
        return command_str

    t0 = PythonOperator(
        task_id='build_cwltool_cmd',
        python_callable=build_cwltool_cmd,
        provide_context=True,
    )

    t1 = BashOperator(
        task_id='launch_cwl_pipeline',
        bash_command="""
            source venv/bin/activate
            # repository name
            echo ${1}
            # workflow file-path (are these all named pipeline.cwl)
            echo ${2}
            # command line args
            echo ${3}
                        
            WORK_DIR="/root/cwl_workflows/"
            echo $WORK_DIR
                        
            toil ssh-cluster --zone us-east-2a hubmap-test-cluster << EOF
                set -x
                source /root/toil_venv/bin/activate
                {{ti.xcom_pull(task_ids='build_cwltool_cmd')}}
                #toil-cwl-runner --provisioner aws --jobStore aws:us-west-2:toil-cluster $WORK_DIR/${1}/${2} ${3}
                #/root/cwl-workflows/ome-tiff-pyramid/pipeline.cwl --ometiff_directory /tmp/ometiff-pyramid-test/
                exit
            EOF
        """
    )



    dag >> t0