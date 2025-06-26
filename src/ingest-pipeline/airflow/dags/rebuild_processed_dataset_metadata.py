from pprint import pprint
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.configuration import conf as airflow_conf

from utils import (
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    create_dataset_state_error_callback,
    make_send_status_msg_function,
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

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")

    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")

    def check_one_uuid(uuid, **kwargs):
        """
        Look up information on the given uuid or HuBMAP identifier.
        Returns:
        - the uuid, translated from an identifier if necessary
        - data type(s) of the dataset
        - local directory full path of the dataset
        """
        print(f"Starting uuid {uuid}")
        my_callable = lambda **kwargs: uuid
        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        print("ds_rslt:")
        pprint(ds_rslt)

        for key in ["status", "uuid", "local_directory_full_path", "metadata"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if not ds_rslt["status"] in ["New", "Error", "QA", "Published", "Submitted"]:
            raise AirflowException(f"Dataset {uuid} is not QA or better")

        return (ds_rslt["uuid"], ds_rslt["local_directory_full_path"], ds_rslt["metadata"])

    def check_uuids(**kwargs):
        print("dag_run conf follows:")
        pprint(kwargs["dag_run"].conf)

        try:
            assert_json_matches_schema(
                kwargs["dag_run"].conf, "launch_checksums_metadata_schema.yml"
            )
        except AssertionError as e:
            print("invalid metadata follows:")
            pprint(kwargs["dag_run"].conf)
            raise

        uuid, lz_path, metadata = check_one_uuid(kwargs["dag_run"].conf["uuid"], **kwargs)
        print(f"filtered metadata: {metadata}")
        print(f"filtered paths: {lz_path}")
        kwargs["dag_run"].conf["lz_path"] = lz_path
        kwargs["dag_run"].conf["src_path"] = airflow_conf.as_dict()["connections"][
            "src_path"
        ].strip("'")

    t_check_uuids = PythonOperator(
        task_id="check_uuids",
        python_callable=check_uuids,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    def additional_md_function(**kwargs):
        """
        This is a hook for future development.  Metadata returned here will
        be updated on the dataset.
        """
        return {}

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=["check_uuids"],
        cwl_workflows=[],
        dataset_uuid_fun=get_dataset_uuid,
        dataset_lz_path_fun=get_dataset_lz_path,
        metadata_fun=additional_md_function,
        include_file_metadata=True,
    )

    def wrapped_send_status_msg(**kwargs):
        if send_status_msg(**kwargs):
            # These xcom values are used to chain to potential downstream workflows-
            # very unlikely in this application but we might as well make sure they exist.
            kwargs["ti"].xcom_push(key="collectiontype", value=None)
            kwargs["ti"].xcom_push(key="assay_type", value=None)

    t_send_status = PythonOperator(
        task_id="send_status_msg",
        python_callable=wrapped_send_status_msg,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "crypt_auth_tok": encrypt_tok(
                airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]
            ).decode(),
        },
    )

    (
        t_check_uuids
        >> t_create_tmpdir
        >> t_send_status  # new metadata is scanned in during this step
        >> t_cleanup_tmpdir
    )

