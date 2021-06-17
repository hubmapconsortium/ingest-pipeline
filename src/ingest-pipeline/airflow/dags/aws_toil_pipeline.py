from airflow import DAG, models
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import globus_sdk

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

with DAG('aws_toil_pipeline', schedule_interval=None, is_paused_upon_creation=False, default_args=default_args) as dag:
    def perform_transfer(*argv, **kwargs):
        ctx = kwargs['dag_run'].conf
        globus_transfer_token = ctx['tokens']['transfer.api.globus.org']['access_token']
        src_epid = 'ff1bd56e-2e65-4ec9-86fa-f79422884e96'
        dest_epid = 'bcc55f9a-63d3-4c05-8da4-7eaa4d215f33'
        authorizer = globus_sdk.AccessTokenAuthorizer(globus_transfer_token)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
        tc.endpoint_autoactivate(src_epid)
        tc.endpoint_autoactivate(dest_epid)

        td = globus_sdk.TransferData(tc, src_epid, dest_epid, label='Transfer')

        for transfer_item in ctx['conf']['transfer_items']:
            try:
                td.add_item(transfer_item['src'], transfer_item['dest'],
                            recursive=transfer_item.get('recursive', False))
            except Exception as e:
                print(e)
                continue

        tc.submit_transfer(td)

    def build_cwltool_cmd(**kwargs):
        ctx = kwargs['dag_run'].conf

        command_str = ''

        for transfer_item in ctx['conf']['transfer_items']:
            command_str += f'aws s3 cp s3://globus-toil-test-bucket/{transfer_item["dest"]} /tmp/{transfer_item["dest"]} ' \
                           f'{"--recursive" if transfer_item.get("recursive", False) else ""} ' + '\\\n'

        command_str += f'toil-cwl-runner --outdir /tmp/{ctx["conf"]["pipeline_name"]}_output --provisioner aws --jobStore aws:us-west-2:toil-cluster ' \
                      f'/root/cwl_workflows/{ctx["conf"]["pipeline_name"]}/pipeline.cwl {ctx["conf"]["cli_args"]}'

        return command_str

    globus_transfer = PythonOperator(
        task_id='perform_transfer',
        python_callable=perform_transfer,
        provide_context=True
    )

    t0 = PythonOperator(
        task_id='build_cwltool_cmd',
        python_callable=build_cwltool_cmd,
        provide_context=True,
    )

    # TODO: Parameterize this command
    #  Cluster name, repository name, input data, s3 bucket name, cli args
    t1 = BashOperator(
        task_id='launch_cwl_pipeline',
        bash_command="""
            toil ssh-cluster --zone us-east-2a jp-lh-hubmap-test-cluster << EOF
                set -x
                source /root/toil_venv/bin/activate
                {{ti.xcom_pull(task_ids='build_cwltool_cmd')}}
                exit
            EOF
        """
    )



    dag >> globus_transfer >> t0 >> t1