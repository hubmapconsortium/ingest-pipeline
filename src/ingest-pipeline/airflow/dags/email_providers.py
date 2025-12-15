import json
import logging
from datetime import datetime, timezone
from os.path import dirname, join
from pathlib import Path

import pandas as pd
from biweekly_timetable import BiweeklyTimetable  # type: ignore
from hubmap_operators.common_operators import (  # type: ignore
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
)
from status_change.callbacks.failure_callback import FailureCallback
from utils import (
    HMDAG,
    encrypt_tok,
    get_auth_tok,
    get_preserve_scratch_resource,
    get_queue_resource,
    get_tmp_dir_path,
    send_email,
)

from airflow.configuration import conf as airflow_conf
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.hooks.http import HttpHook

INTERNAL_CONTACTS = ["gesina@psc.edu"]
DP_PATH = Path(join(dirname(__file__), "data_providers.json"))


default_args = {
    "depends_on_past": False,
    "email": ["gesina@psc.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": FailureCallback(__name__),
    "owner": "hubmap",
    "queue": get_queue_resource("email_providers"),
    "retries": 0,
    "start_date": datetime(2025, 12, 1, tzinfo=timezone.utc),
    "xcom_push": True,
}

with HMDAG(
    dag_id="email_providers",
    default_args=default_args,
    is_paused_upon_creation=False,
    schedule=BiweeklyTimetable(),
    catchup=False,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("email_providers"),
    },
    params={
        "token": get_auth_tok(
            crypt_auth_tok=encrypt_tok(
                str(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"])
            ).decode()
        )
    },
) as dag:

    @task
    def get_groups(**kwargs):
        """
        Get data provider group contact info.
        Format:
            {
                group_name: {
                    "uuid": uuid,
                    "contacts": {
                        contact_name: contact_email
                    }
                }
            }
        """
        if DP_PATH.exists():
            with open(DP_PATH, "r") as f:
                groups = json.load(f)
        else:
            logging.error("No groups found.")
            groups = {}
        kwargs["ti"].xcom_push(key="groups", value=groups)

    @task.branch(task_id="branch_task")
    def send_data(**kwargs):
        """
        - Format data and send to each group.
        - Push errors dict to XCOM.
        - Return task_id of next step:
            "skip_task": no issue getting/sending data
            "report_errors": error getting/sending data
        """
        groups = kwargs["ti"].xcom_pull(key="groups")
        data = get_data(groups, kwargs.get("token", ""))
        errors = {}
        for group_name, group_attrs in groups.items():
            try:
                primary_contacts = list(group_attrs.get("contacts", {}).values())
                group_data = pd.DataFrame.from_dict(data.get(group_name, {}))
                email_body = format_group_data(group_data, group_name)
                spreadsheet_path = get_csv_path(group_name, "{{ run_id }}")
                group_data.to_csv(spreadsheet_path)
                cc = list(set(group_data["creator_email"].tolist()))
                date = datetime.now().strftime("%Y-%m-%d")
                send_email(
                    primary_contacts,
                    f"HuBMAP dataset status report ({date})",
                    email_body,
                    attachment_path=spreadsheet_path,
                    cc=cc,
                )
                logging.info(
                    f"Email for {group_name} sent to primary contacts {primary_contacts}."
                )
            except Exception as e:
                logging.error(f"{group_name}: {str(e.__class__)}: {e}")
                errors[group_name] = str(e)
        kwargs["ti"].xcom_push(key="errors", value=errors)
        if errors:
            return "report_errors"
        return "skip_task"

    @task
    def report_errors(**kwargs):
        """
        Log errors cleanly and email internal contact.
        """
        errors = kwargs["ti"].xcom_pull(key="errors")
        formatted_errors = "<br>".join([f"{key}: {val}" for key, val in errors.items()])
        logging.error(formatted_errors)
        send_email(
            INTERNAL_CONTACTS,
            "Errors in EmailProviders DAG",
            formatted_errors,
        )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_get_groups = get_groups()
    t_send_data = send_data()
    t_report_errors = report_errors()
    t_skip_task = EmptyOperator(task_id="skip_task")
    t_cleanup_tmpdir = CleanupTmpDirOperator(
        task_id="cleanup_temp_dir", trigger_rule="none_failed_min_one_success"
    )

########################
# Supporting functions #
########################


def get_data(groups: dict, token: str) -> dict:
    """
    Get data from search API by group.
    """
    data = {}
    for group_name, group_data in groups.items():
        data[group_name] = get_datasets_by_group(group_name, group_data["uuid"], token).to_dict()
    return data


def get_datasets_from_search_api(
    group_field: str, group_field_val: str, token: str
) -> pd.DataFrame:
    source_fields = [
        "created_by_user_display_name",
        "created_by_user_email",
        "created_timestamp",
        "creation_action",
        "dataset_type",
        "entity_type",
        "group_name",
        "group_uuid",
        "hubmap_id",
        "last_modified_timestamp",
        "status",
        "title",
    ]
    body = {
        "_source": source_fields,
        "size": 10000,
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {"creation_action": "Create Dataset Activity"}
                    },  # Primary datasets only
                ],
                "must": [
                    {"match": {"entity_type": "Dataset"}},
                    {"match": {group_field: group_field_val}},
                ],
                "must_not": [
                    {"match": {"status": "Published"}},
                ],
            }
        },
    }

    data = search_api_request(body, token)
    df = pd.json_normalize(data, record_path=["hits", "hits"])
    # Rename columns for clarity
    df = df.rename(
        columns={
            "_id": "uuid",
            **{f"_source.{source_field}": source_field for source_field in source_fields},
        }
    )
    return df


def search_api_request(body: dict, token: str) -> dict:
    try:
        search_hook = HttpHook("POST", http_conn_id="search_api_connection")
        headers = {"authorization": f"Bearer {token}"}
        response = search_hook.run(endpoint="v3/portal/search", headers=headers, json=body)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Error querying Search API: {e}")
    return response.json()


# def search_api_request(body: dict, token: str) -> dict:
#     import requests
#
#     try:
#         headers = {"authorization": f"Bearer {token}"}
#         response = requests.post(
#             url="https://search.api.hubmapconsortium.org/v3/portal/search",
#             headers=headers,
#             json=body,
#         )
#         response.raise_for_status()
#     except Exception as e:
#         raise Exception(f"Error querying Search API: {e}")
#     return response.json()


def get_datasets_by_group(group_name: str, group_uuid: str, token: str) -> pd.DataFrame:
    """
    Fetch all unpublished datasets from Search API by group_uuid.

    Returns:
        DataFrame with upload/dataset information including uuid, created_by, group_name, etc.
    """
    logging.info(f"Fetching unpublished datasets for group {group_name} from Search API...")

    df1 = modify_df(get_datasets_from_search_api("group_uuid", group_uuid, token))
    df2 = modify_df(get_datasets_from_search_api("group_name", group_name, token))
    print(df1.compare(df2))
    return df1


def modify_df(df: pd.DataFrame) -> pd.DataFrame:
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
        "creator_email",
        "ingest_url",
    ]

    df = df[[col for col in columns_to_keep if col in df.columns]]
    logging.info(f"   Found {len(df)} unpublished datasets in Search API")

    assert isinstance(df, pd.DataFrame)
    return df


##############
# Formatting #
##############


def format_group_data(data: pd.DataFrame, group_name: str) -> str:
    dataset_info = get_email_body_list(data)
    body = [*get_template(data, group_name), dataset_info]
    if len(data) > 100:
        body.append(f"... ({len(data) - 100} more datasets, see CSV attachment)")
    return "".join(body)


def get_template(data: pd.DataFrame, group_name: str) -> list[str]:
    return [
        f"<b>Biweekly unpublished dataset report for {group_name}</b><br>",
        "This report is sent to the group PI and all owners of datasets in this list.<br>",
        "<br>",
        f"{len(data)} unpublished datasets:<br>",
        "<ul>",
        *get_counts(data),
        "</ul>",
        # TODO
        # "Instructions:",
        "<br>",
        "<br>",
    ]


def create_link(row):
    return f'<a href="{row.ingest_url}">{row.hubmap_id}</a>'


###########
# Parsing #
###########


def get_csv_path(group_name: str, run_id: str) -> str:
    date = datetime.now().strftime("%Y-%m-%d")
    group_name_formatted = group_name.replace(" - ", "_").replace(" ", "_")
    return str(get_tmp_dir_path(f"{run_id}/{group_name_formatted}_{date}.csv"))


def get_date(row, column: str) -> str:
    timestamp = pd.to_datetime(row[column], unit="ms")
    return datetime.strftime(timestamp, "%Y-%m-%d")


def get_ingest_url(row) -> str:
    # PROD only
    if row.get("entity_type") and row.get("uuid"):
        return f"https://ingest.hubmapconsortium.org/{row['entity_type']}/{row['uuid']}"
    return ""


def get_email_body_list(data: pd.DataFrame) -> str:
    subset = data[["hubmap_id", "last_modified_date", "status", "ingest_url"]].copy()
    subset["hubmap_id"] = subset.apply(create_link, axis=1)
    subset = subset.sort_values("last_modified_date")
    subset = subset.rename(
        columns={
            "hubmap_id": "HuBMAP ID",
            "status": "Status",
            "last_modified_date": "Last Updated",
        }
    )
    return subset.to_html(
        columns=["HuBMAP ID", "Last Updated", "Status"],
        index=False,
        na_rep="",
        justify="justify",
        max_rows=100,
        escape=False,
        border=0,
        col_space={"HuBMAP ID": 170, "Last Updated": 130},
    ).replace("\n", "")


def get_counts(data: pd.DataFrame) -> list[str]:
    counts = data["status"].value_counts().to_dict()
    provider_responsible_keys = ["qa", "invalid"]
    provider_responsible = {
        key: value for key, value in counts.items() if key.lower() in provider_responsible_keys
    }
    iec_responsible = {
        key: value for key, value in counts.items() if key.lower() not in provider_responsible_keys
    }
    return [
        "Datasets requiring action by data provider:",
        "<ul>",
        *[f"<li>{key}: {value}</li>" for key, value in provider_responsible.items()],
        "</ul>",
        "Datasets requiring action by IEC:",
        "<ul>",
        *[f"<li>{key}: {value}</li>" for key, value in iec_responsible.items()],
        "</ul>",
    ]


############
# Workflow #
############

(t_create_tmpdir >> t_get_groups >> t_send_data >> [t_report_errors, t_skip_task] >> t_cleanup_tmpdir)  # type: ignore
