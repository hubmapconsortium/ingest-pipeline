from typing import Tuple

from status_change.status_utils import get_abs_path, get_organ

from .base import SlackMessage


class SlackUploadReorganized(SlackMessage):
    name = "upload_reorganized"

    def __init__(self, uuid, token, entity_data=None):
        super().__init__(uuid, token, entity_data)
        self.datasets: list[dict] = self.entity_data.get("datasets", [])

    @property
    def dataset_vals(self):
        return [
            self.entity_id_str,
            "created_by_user_displayname",
            "created_by_user_email",
            "dataset_type",
        ]

    def get_non_upload_metadata(self):
        self.dataset_type = self.datasets[0].get("dataset_type") if self.datasets else None
        self.organ = (
            get_organ(self.datasets[0].get("uuid", ""), self.token) if self.datasets else None
        )

    def _format_upload_reorganized_datasets(self) -> list[str]:
        """
        Format dataset list portion of message, each dataset on one row.
        Override dataset_vals property to change fields pulled from metadata.
        """
        if not self.datasets:
            return [""]
        # Add keys as index 0.
        info = [",".join([*self.dataset_vals, "organ", "globus_link", "filesystem_path"])]
        for dataset in self.datasets:
            data = [dataset.get(key, "") for key in self.dataset_vals]
            uuid = dataset.get("uuid", "")
            # Organ and globus_url are derived from additional API calls.
            data.append(get_organ(uuid, self.token))
            data.append(f"<{self.get_globus_url(uuid)}|Globus>")
            data.append(get_abs_path(uuid, self.token, escaped=True))
            info.append(", ".join(self._clean_dataset_rows(data)))
        return info

    def _clean_dataset_rows(self, dataset_rows: list) -> list:
        # Datasets should be formatted as lines of comma-separated strings
        # with internal ; delimiters.
        cleaned_data = []
        for val in dataset_rows:
            if isinstance(val, list):
                cleaned_data.append(";".join(val))
            elif isinstance(val, str):
                cleaned_data.append(val.replace(",", ";"))
            else:
                cleaned_data.append(str(val))
        return cleaned_data

    def format(self) -> str:
        dataset_info, msg_data = self.get_upload_info()
        return f"""
        Upload {self.uuid} reorganized:
        {self.get_combined_info(msg_data, dataset_info)}
        """

    def get_upload_info(self) -> Tuple[list, dict]:
        self.get_non_upload_metadata()
        dataset_info = self._format_upload_reorganized_datasets()
        msg_data = {
            self.entity_id_str: f"<{self.ingest_ui_url}|{self.entity_data.get(self.entity_id_str)}>",  # entity id with link to Ingest UI
            "created_by_user_displayname": self.entity_data.get("created_by_user_displayname"),
            "created_by_user_email": self.entity_data.get("created_by_user_email"),
            "dataset_type": self.dataset_type,
            "organ": self.organ,
        }
        return dataset_info, msg_data

    def get_combined_info(self, data: dict, dataset_info: list):
        return (
            "\n   ".join([f"{key}: {value}" for key, value in data.items()])
            + "\n\nDatasets:\n"
            + "\n".join(dataset_info)
        )


class SlackUploadReorganizedPriority(SlackUploadReorganized):
    """
    Reorganized priority project (SWAT, MOSDAP) uploads.
    """

    name = "upload_priority_reorganized"

    @property
    def dataset_vals(self):
        return [
            self.entity_id_str,
            "created_by_user_displayname",
            "created_by_user_email",
            "priority_project_list",
            "dataset_type",
        ]

    @classmethod
    def test(cls, entity_data, token) -> bool:
        del token
        return bool(entity_data.get("priority_project_list"))

    def format(self) -> str:
        """
        Formats data for priority project reorganization Slack message.
        Prioritizes returning a message over tracking down missing data.
        """
        dataset_info, msg_data = self.get_upload_info()
        priority_projects_list = ", ".join(self.entity_data.get("priority_project_list", []))
        msg_data["priority_project_list"] = priority_projects_list

        return f"""
        Priority upload ({priority_projects_list}) reorganized:
        {self.get_combined_info(msg_data, dataset_info)}
        """
