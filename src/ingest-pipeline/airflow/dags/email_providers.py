import json
import logging
from datetime import datetime, timezone
from os.path import dirname, join
from pathlib import Path

import pandas as pd
from hubmap_operators.common_operators import (
    CleanupTmpDirOperator,
    CreateTmpDirOperator,
)
from status_change.callbacks.failure_callback import FailureCallback
from status_change.status_utils import Statuses
from timetables.biweekly_timetable import BiweeklyTimetable
from utils import (
    HMDAG,
    decrypt_tok,
    encrypt_tok,
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
"""
data_providers.json lists all sites who have contributed datasets
as of 2025-12, with the exception of components that no longer have
funding and that no other component has taken over (RTI Broad and
California Institute of Technology TMC).
"""
DP_PATH = Path(join(dirname(__file__), "data_providers.json"))


default_args = {
    "depends_on_past": False,
    "email": ["gesina@psc.edu"],
    "email_on_failure": False,
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
        "preserve_scratch": get_preserve_scratch_resource("geomx"),
    },
    params={
        "crypt_auth_tok": encrypt_tok(
            str(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"])
        ).decode()
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
                    },
                    "subcomponent": {  # optional
                        "subcomponent_name": {
                            "uuid": uuid
                        }
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

    @task.branch(task_id="send_data")
    def send_data(**kwargs):
        """
        - Format data and send to each group.
        - Push errors dict to XCOM.
        - Return task_id of next step:
            "skip_task": no issue getting/sending data
            "report_errors": error getting/sending data
        """
        groups = kwargs["ti"].xcom_pull(key="groups")
        token = "".join(
            e for e in decrypt_tok(dag.params["crypt_auth_tok"].encode()) if e.isalnum()
        )  # strip out non-alnum characters
        errors = {}
        try:
            data = get_data(groups, token)
        except Exception as e:
            logging.error(e)
            errors["Data error"] = str(e)
            data = {}
        if data:
            for group_name, group_attrs in groups.items():
                try:
                    if not data.get(group_name):
                        continue
                    primary_contacts = list(group_attrs.get("contacts", {}).values())
                    group_data = pd.DataFrame.from_dict(data.get(group_name, {}))
                    email_body = format_group_data(group_data, group_name)
                    spreadsheet_path = get_csv_path(group_name, kwargs["run_id"])
                    group_data.to_csv(spreadsheet_path, index=False)
                    cc = group_data.created_by_user_email.unique().tolist()
                    if "hubmap@hubmapconsortium.org" in cc:
                        cc.remove("hubmap@hubmapconsortium.org")
                    date = datetime.now().strftime("%Y-%m-%d")
                    sent = send_email(
                        primary_contacts,
                        f"HuBMAP dataset status report ({date})",
                        email_body,
                        attachment_path=spreadsheet_path,
                        cc=cc,
                    )
                    if sent:
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
            # prod_only=False
        )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_get_groups = get_groups()
    t_send_data = send_data()
    t_report_errors = report_errors()
    t_skip_task = EmptyOperator(task_id="skip_task")
    t_cleanup_tmpdir = CleanupTmpDirOperator(
        task_id="cleanup_temp_dir", trigger_rule="none_failed_min_one_success"
    )

##############
# Search API #
##############


def get_data(groups: dict, token: str) -> dict:
    """
    Get data from search API by group.
    """
    data = {}
    for group_name, group_info in groups.items():
        group_data = get_datasets_by_group(group_name, group_info["uuid"], token)
        if group_data is not None:
            data[group_name] = group_data.to_dict()
            if subcomponent := group_info.get("subcomponent"):
                for subcomponent_name, subcomponent_info in subcomponent.items():
                    if subcomponent_data := get_datasets_by_group(
                        subcomponent_name,
                        subcomponent_info["uuid"],
                        token,
                    ):
                        data[group_name].update(subcomponent_data.to_dict())
        else:
            data[group_name] = {}
    return data


def search_api_request(body: dict, token: str) -> dict:
    try:
        search_hook = HttpHook("POST", http_conn_id="search_api_connection")
        headers = {"authorization": f"Bearer {token}"}
        response = search_hook.run(endpoint="v3/portal/search", headers=headers, json=body)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Error querying Search API: {e}")
    return response.json()


def get_datasets_by_group(group_name: str, group_uuid: str, token: str) -> pd.DataFrame | None:
    """
    Fetch all unpublished datasets from Search API by group_uuid.

    Returns:
        DataFrame with upload/dataset information including uuid, created_by, group_name, etc.
    """
    logging.info(f"Fetching unpublished datasets for group {group_name} from Search API...")
    data = search_api_request(get_search_body(group_uuid), token)
    if not data["hits"]["hits"]:
        logging.info(f"   No unpublished datasets found for {group_name}.")
        return None
    df = pd.json_normalize(data, record_path=["hits", "hits"])
    verify_search_results(df, group_name)
    df = modify_df(df)

    logging.info(f"   Found {len(df)} unpublished datasets in Search API for {group_name}.")

    return df


search_fields: list[str] = [
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


def get_search_body(group_uuid: str) -> dict:
    return {
        "_source": search_fields,
        "size": 10000,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"creation_action.keyword": {"value": "Create Dataset Activity"}}
                    },  # Primary datasets only
                    {"term": {"entity_type.keyword": {"value": "Dataset"}}},
                    {"term": {"group_uuid.keyword": {"value": group_uuid}}},
                ],
                "must_not": [
                    {"term": {"status.keyword": {"value": "Published"}}},
                ],
            }
        },
    }


def verify_search_results(df: pd.DataFrame, group_name: str):
    # Verify that results are for the correct group
    unique_groups_found = df["_source.group_name"].unique()
    assert (
        len(unique_groups_found) == 1
    ), f"Not all results are for {group_name}. Groups in results: {', '.join(unique_groups_found)}."
    assert (
        unique_groups_found[0] == group_name
    ), f"Results do not match {group_name}. Group in results: {unique_groups_found[0]}."


#####################
# Prepare DataFrame #
#####################


def modify_df(df: pd.DataFrame) -> pd.DataFrame:
    # Rename columns for clarity
    df = df.rename(
        columns={
            "_id": "uuid",
            **{f"_source.{source_field}": source_field for source_field in search_fields},
        }
    )

    # Create/modify fields
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
        "created_by_user_display_name",
        "created_by_user_email",
        "ingest_url",
    ]

    df = df[[col for col in columns_to_keep if col in df.columns]]
    return df


def get_csv_path(group_name: str, run_id: str) -> str:
    date = datetime.now().strftime("%Y-%m-%d")
    group_name_formatted = group_name.replace(" - ", "_").replace(" ", "_")
    return str(get_tmp_dir_path(run_id) / f"{group_name_formatted}_{date}.csv")


def get_date(row: pd.Series, column: str) -> str:
    timestamp = pd.to_datetime(row[column], unit="ms")
    return datetime.strftime(timestamp, "%Y-%m-%d")


def get_ingest_url(row: pd.Series) -> str:
    # PROD only
    if row.get("entity_type") and row.get("uuid"):
        return f"https://ingest.hubmapconsortium.org/{row['entity_type']}/{row['uuid']}"
    return ""


#################
# Prepare email #
#################

max_rows: int = 20


def format_group_data(data: pd.DataFrame, group_name: str) -> str:
    body = get_template(data, group_name)
    return "".join(body)


def get_template(data: pd.DataFrame, group_name: str) -> list[str]:
    # Default template
    template = [
        f"<b>Biweekly unpublished dataset report for {group_name}</b><br>",
        "This report is sent to the group PI(s) and all creators of datasets in this list.<br>",
        "<br>",
        f"If you have questions, please schedule an appointment with Data Curator Brendan Honick (https://calendly.com/bhonick-psc/) or email ingest@hubmapconsortium.org. Do not respond to this email; this inbox is not monitored.<br>",
        "<br>",
        f"<b>Unpublished datasets:</b> {len(data)}<br>",
        "You can see more details about all datasets in the attached CSV file.<br>",
        # "<br>",
        # "<b>Counts by status:</b>",
        # *get_counts(data),
    ]
    # If counts exist for new, invalid, or qa: add instructions
    instructions = add_instructions(data)
    template.extend(instructions)
    # Include counts of statuses without instructions, if present
    other_counts = get_counts(
        data, exclude=[Statuses.DATASET_QA, Statuses.DATASET_INVALID, Statuses.DATASET_NEW]
    )
    if other_counts:
        if instructions:
            template.append("<b><mark>Counts of datasets with other statuses</mark></b>:")
        else:
            template.append("<b>Datasets by status:</b>")
        template.extend(["<ul>", *other_counts, "</ul>"])
    # Footer
    template.extend(
        [
            "Please consult our <a href='https://docs.google.com/document/d/13zBbp_BNCVPZPj71Q5dGLoNJ_TDRyEpPck2mGQbkLYg/edit?usp=sharing'>guide to HuBMAP entity statuses</a> if you would like to learn more about the different dataset statuses.<br>",
        ]
    )
    return template


def add_instructions(data: pd.DataFrame) -> list[str]:
    qa = get_counts(data, [Statuses.DATASET_QA])
    invalid = get_counts(data, [Statuses.DATASET_INVALID])
    new = get_counts(data, [Statuses.DATASET_NEW])
    if not any([qa, invalid, new]):
        return []
    template = [
        "<br>",
        "<b>What you can do to move datasets forward:</b><br>",
        "Some dataset statuses indicate the need for intervention by the data provider. Below are some brief instructions by status.<br>",
        "<ul>",
    ]
    if qa:
        template.extend(
            [
                qa[0],
                "The dataset has been validated and is ready for site approval. <mark>ADD INSTRUCTION HERE.</mark><br>",
                "<ul>",
                *[f"<li>{dataset}</li>" for dataset in list_datasets_by_status(data, "QA")],
                "</ul>",
                "<br>",
            ]
        )
    if invalid:
        template.extend(
            [
                invalid[0],
                "The dataset cannot be validated due a metadata or directory issue. View the errors on the dataset ingest page and correct them. Help is available by scheduling with Data Curator Brendan Honick (https://calendly.com/bhonick-psc/).<br>",
                "<ul>",
                *[f"<li>{dataset}</li>" for dataset in list_datasets_by_status(data, "Invalid")],
                "</ul>",
                "<br>",
            ]
        )
    if new:
        template.extend(
            [
                new[0],
                'This status is an artifact of prior status handling. Go to the dataset ingest page and press "Submit."<br>',
                "<ul>",
                *[f"<li>{dataset}</li>" for dataset in list_datasets_by_status(data, "New")],
                "</ul>",
            ]
        )
    template.extend(
        [
            "</ul>",
            "<br>",
        ]
    )
    return template


def create_link(row: pd.Series) -> str:
    return f'<a href="{row.ingest_url}">{row.hubmap_id}</a>'


def list_datasets_by_status(data: pd.DataFrame, status: str) -> list[str]:
    subset = data[["hubmap_id", "last_modified_date", "status", "ingest_url"]].copy()
    subset["hubmap_id"] = subset.apply(create_link, axis=1)
    subset = subset.sort_values("last_modified_date")
    subset = subset.rename(columns={"hubmap_id": "HuBMAP ID"})
    filtered = subset.loc[subset["status"] == status]
    id_list = [val["HuBMAP ID"] for val in filtered.to_dict(orient="index").values()]
    if len(id_list) > max_rows:
        full_len = len(id_list)
        id_list = id_list[0:max_rows]
        id_list.append(f"...{full_len - max_rows} more datasets, see CSV attachment")
    return id_list


def get_counts(
    data: pd.DataFrame,
    include: list[Statuses] = [],
    exclude: list[Statuses] = [],
) -> list[str]:
    counts = data["status"].value_counts().to_dict()
    if include:
        str_include = [status.status_str for status in include]
        counts = {key: value for key, value in counts.items() if key.lower() in str_include}
    str_exclude = [status.status_str for status in exclude]
    counts = {key: value for key, value in counts.items() if key.lower() not in str_exclude}
    count_list = [f"<li>{key}: {value}</li>" for key, value in counts.items()]
    count_list.sort()
    return count_list


############
# Workflow #
############

(t_create_tmpdir >> t_get_groups >> t_send_data >> [t_report_errors, t_skip_task] >> t_cleanup_tmpdir)  # type: ignore
