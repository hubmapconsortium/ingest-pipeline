from typing import Optional

from airflow.configuration import conf as airflow_conf
from airflow.providers.http.hooks.http import HttpHook

from .status_utils import get_submission_context

########################################
# Reorganized priority project uploads #
########################################


def format_priority_reorganized_msg(token: str, uuid: str) -> tuple[str, str]:
    """
    Formats data for priority project reorganization Slack message.
    Prioritizes returning a message over tracking down missing data.
    Retrieves channel ID from app.cfg.
    """
    # Re-query data to get post-reorganization updated data
    entity_data = get_submission_context(token, uuid)
    priority_reorganized_channel = str(
        airflow_conf.as_dict().get("slack_channels", {}).get("PRIORITY_UPLOAD_REORGANIZED", "")
    )
    datasets = entity_data.get("datasets", [])
    dataset_type = datasets[0].get("dataset_type") if datasets else None
    organ = get_organ(datasets[0].get("uuid"), token) if datasets else None
    dataset_info = _format_upload_reorganized_datasets(datasets, token)
    priority_projects_list = ", ".join(entity_data.get("priority_project_list", []))
    data = {
        "uuid": entity_data.get("uuid"),
        "hubmap_id": entity_data.get("hubmap_id"),
        "created_by_user_displayname": entity_data.get("created_by_user_displayname"),
        "created_by_user_email": entity_data.get("created_by_user_email"),
        "priority_project_list": priority_projects_list,
        "dataset_type": dataset_type,
        "organ": organ,
    }
    msg = _format_full_priority_reorganized_msg(priority_projects_list, data, dataset_info)
    return msg, priority_reorganized_channel


def _format_full_priority_reorganized_msg(
    upload_label: str, data: dict[str, str], dataset_info: list[str]
) -> str:
    print_vals = (
        "\n   ".join([f"{key}: {value}" for key, value in data.items()])
        + "\n\nDatasets:\n"
        + "\n".join(dataset_info)
    )
    return f"""Priority upload ({upload_label}) reorganized:\n   {print_vals}"""


def _format_upload_reorganized_datasets(datasets: list[dict], token: str) -> list[str]:
    """
    Formatted according to priority upload needs but
    generally usable for reorganized uploads.
    """
    if not datasets:
        return [""]
    keys = [
        "hubmap_id",
        "created_by_user_displayname",
        "created_by_user_email",
        "priority_project_list",
        "dataset_type",
        "organ",
    ]
    info = [", ".join(keys)]
    for dataset in datasets:
        # Organ is derived from additional API call, don't
        # want it in the keys we look for in dataset
        data = [dataset.get(key, "") for key in keys if key != "organ"]
        organ = get_organ(dataset.get("uuid", ""), token)
        data.append(organ if organ else "")
        # Datasets should be formatted as lines of comma-separated strings
        # with internal ; delimiters.
        cleaned_data = []
        for val in data:
            if isinstance(val, list):
                cleaned_data.append(";".join(val))
            elif isinstance(val, str):
                cleaned_data.append(val.replace(",", ";"))
            else:
                cleaned_data.append(str(val))
        info.append(", ".join(cleaned_data))
    return info


#########
# Utils #
#########


def get_organ(uuid: str, token: str) -> Optional[str]:
    """
    Get ancestor organ for sample, dataset, or publication.
    """
    http_hook = HttpHook("GET", http_conn_id="entity_api_connection")
    response = http_hook.run(
        f"/entities/{uuid}/ancestor-organs", headers={"Authorization": "Bearer " + token}
    )
    try:
        response.raise_for_status()
        return response.json()[0].get("organ")
    except Exception as e:
        print(e)
        return None
