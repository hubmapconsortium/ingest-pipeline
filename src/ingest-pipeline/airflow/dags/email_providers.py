import csv
from datetime import datetime, timedelta

import pandas as pd
import requests
from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
)
from plugins.biweekly_timetable import BiweeklyTimetable
from status_change.callbacks.failure_callback import FailureCallback
from utils import (
    HMDAG,
    get_auth_tok,
    get_preserve_scratch_resource,
    get_queue_resource,
    get_tmp_dir_path,
)

from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.hooks.http import HttpHook

SEARCH_API_URL = "https://search.api.hubmapconsortium.org/v3/portal/search"

default_args = {
    "depends_on_past": False,
    "email": ["gesina@psc.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": FailureCallback(__name__),
    "owner": "hubmap",
    "queue": get_queue_resource("email_providers"),
    "retries": 0,
    "start_date": datetime(2019, 1, 1),
    "xcom_push": True,
}

with HMDAG(
    "email_providers",
    default_args=default_args,
    is_paused_upon_creation=False,
    schedule=BiweeklyTimetable(),
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("validate_upload"),
    },
) as dag:

    def get_groups(**kwargs):
        # TODO: just email or email/name?
        """
        Get data provider group info and push to XCOM.
        """
        groups = []
        kwargs["ti"].xcom_push(key="groups", value=groups)

    t_get_groups = PythonOperator(
        task_id="get_groups",
        python_callable=get_groups,
        provide_context=True,
    )

    def get_send_data(**kwargs):
        """
        - Get data from search API, format, and send to each group.
        - Push return code to XCOM:
            0: no issue getting/sending data
            1: error getting/sending data
        - Push errors dict to XCOM.
        """
        errors = {}
        groups = kwargs["ti"].xcom_pull(key="groups")
        for group_name, group_contact in groups.items():
            data = get_datasets_by_group(group_name)
            email_body = format_group_data(data)
            spreadsheet = format_csv(data)
            # collect any errors with data or send to report
            assert group_contact, email_body
            send_email(group_contact, email_body, attachment=spreadsheet)
        kwargs["ti"].xcom_push(key="send_success", value="1" if errors else "0")
        kwargs["ti"].xcom_push(key="errors", value=errors)

    t_get_send_data = BranchPythonOperator(
        task_id="get_data",
        python_callable=get_send_data,
        provide_context=True,
        op_kwargs={
            "next_op": "cleanup_temp_dir",
            "bail_op": "report_errors",
            # TODO: not sure about test_op
            "test_op": "get_send_data",
        },
    )

    def report_errors(**kwargs):
        errors = kwargs["ti"].xcom_pull(key="errors")
        # TODO: maybe email somebody here

    t_report_errors = PythonOperator(
        task_id="report_errors",
        python_callable=report_errors,
        provide_context=True,
    )

    ########################
    # Supporting functions #
    ########################

    def get_datasets_by_group(group_name: str, **kwargs) -> pd.DataFrame:
        """
        Fetch all unpublished datasets from Search API by group_name.

        Returns:
            DataFrame with upload/dataset information including uuid, created_by, group_name, etc.
        """
        print(f"Fetching unpublished datasets for group {group_name} from Search API...")

        body = {
            "_source": [
                "contacts",  # TODO: ?
                "created_by_user_display_name",
                "created_by_user_email",
                "created_by_user_sub",  # TODO: ?
                "created_timestamp",
                "creation_action",
                "dataset_type",
                "direct_ancestors",  # TODO: not sure
                "entity_type",
                "group_name",
                "group_uuid",  # TODO: check
                "hubmap_id",  # TODO: make agnostic?
                "last_modified_timestamp",
                "status",
                "title",
                "uuid",
            ],
            "size": 10000,  # Adjust if you have more than 10k datasets  # TODO: check limit
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {"creation_action": "Create Dataset Activity"}
                        },  # Primary datasets  # TODO: check this
                    ],
                    "must": [
                        {"match": {"entity_type": "Dataset"}},
                        {"match": {"group_name": group_name}},
                    ],
                    "must_not": [
                        {"match": {"status": "Published"}},
                    ],
                }
            },
        }

        data = search_api_request(body, **kwargs)
        df = pd.json_normalize(data, record_path=["hits", "hits"])

        # Rename columns for clarity
        df = df.rename(
            columns={
                "_id": "uuid",
                "_source.contacts": "contacts",
                "_source.created_by_user_display_name": "creator",
                "_source.created_by_user_email": "creator_email",
                "_source.created_by_user_sub": "created_by_user_sub",
                "_source.created_timestamp": "created_timestamp",
                "_source.creation_action": "creation_action",
                "_source.dataset_type": "dataset_type",
                "_source.entity_type": "entity_type",
                "_source.group_name": "group_name",
                "_source.group_uuid": "group_uuid",
                "_source.hubmap_id": "hubmap_id",
                "_source.last_modified_timestamp": "last_modified_timestamp",
                "_source.status": "status",
                "_source.title": "title",
            }
        )

        df["ingest_url"] = df.apply(get_ingest_url, axis=1)
        df["created_date"] = df.apply(get_date, args=("created_timestamp",), axis=1)
        df["last_modified_date"] = df.apply(get_date, args=("last_modified_timestamp",), axis=1)

        # Keep only relevant columns
        columns_to_keep = [
            "hubmap_id",
            "status",
            "dataset_type",
            "title",
            "created_date",
            "last_modified_date",
            "creator_name",
            "ingest_url",
        ]

        df = df[[col for col in columns_to_keep if col in df.columns]]
        print(f"   Found {len(df)} published datasets in Search API")

        assert isinstance(df, pd.DataFrame)
        return df

    def format_group_data(data: pd.DataFrame) -> str:
        pass

    def format_csv(data: pd.DataFrame):
        # TODO: typing?
        pass

    def send_email(contact: str, email_body: str, attachment=None):
        pass

    #########
    # Utils #
    #########

    def get_date(row, column: str) -> str:
        timestamp = pd.to_datetime(row[column], unit="ms")
        return datetime.strftime(timestamp, "%Y-%m-%d")

    def get_ingest_url(row) -> str:
        if row.get("entity_type") and row.get("uuid"):
            return f"https://ingest.hubmapconsortium.org/{row['entity_type']}/{row['uuid']}"
        return ""

    def search_api_request(body: dict, **kwargs) -> dict:
        headers = {"authorization": f"Bearer {kwargs.get('auth_tok')}"}
        try:
            response = requests.post(url=SEARCH_API_URL, headers=headers, json=body, timeout=60)
            response.raise_for_status()
        except Exception as e:
            # TODO
            raise
        return response.json()

    # def search_api_request(body: dict, **kwargs) -> dict:
    #     try:
    #         search_hook = HttpHook("POST", http_conn_id="search_api_connection")
    #         headers = {"authorization": f"Bearer {get_auth_tok(**kwargs)}"}
    #         response = search_hook.run(url=SEARCH_API_URL, headers=headers, json=body, timeout=60)
    #         response.raise_for_status()
    #     except Exception as e:
    #         # TODO
    #         raise
    #     return response.json()

    ############
    # Workflow #
    ############

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")

    (t_create_tmpdir >> t_get_groups >> t_get_send_data >> t_cleanup_tmpdir)
    t_get_send_data >> t_report_errors
