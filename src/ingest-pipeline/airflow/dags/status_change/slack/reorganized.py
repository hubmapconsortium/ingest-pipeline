from status_change.status_utils import get_globus_url, get_organ

from .base import SlackMessage


class SlackReorganized(SlackMessage):
    name = "upload_reorganized"

    def __init__(self, uuid: str, token: str):
        super().__init__(uuid, token)
        self.datasets = self.entity_data.get("datasets", [])

    @property
    def dataset_vals(self):
        return [
            "hubmap_id",
            "created_by_user_displayname",
            "created_by_user_email",
            "dataset_type",
        ]

    def get_non_upload_metadata(self):
        self.dataset_type = self.datasets[0].get("dataset_type") if self.datasets else None
        self.organ = get_organ(self.datasets[0].get("uuid"), self.token) if self.datasets else None

    def _format_upload_reorganized_datasets(self) -> list[str]:
        """
        Format dataset list portion of message, each dataset on one row.
        Override dataset_vals property to change fields pulled from metadata.
        """
        if not self.datasets:
            return [""]
        # Add keys as index 0.
        info = [",".join([*self.dataset_vals, "organ", "globus_url"])]
        for dataset in self.datasets:
            data = [dataset.get(key, "") for key in self.dataset_vals]
            # Organ and globus_url are derived from additional API calls.
            data.append(get_organ(dataset.get("uuid", ""), self.token))
            data.append(get_globus_url(dataset.get("uuid", ""), self.token))
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

    # TODO
    # def format(self, **kwargs) -> str:
    #     return ""


class SlackPriorityReorganized(SlackReorganized):
    """
    Reorganized priority project (SWAT, MOSDAP) uploads.
    """

    name = "priority_upload_reorganized"

    @property
    def dataset_vals(self):
        return [
            "hubmap_id",
            "created_by_user_displayname",
            "created_by_user_email",
            "priority_project_list",
            "dataset_type",
        ]

    @classmethod
    def test(cls, entity_data) -> bool:
        return bool(entity_data.get("priority_project_list"))

    def format(self) -> str:
        """
        Formats data for priority project reorganization Slack message.
        Prioritizes returning a message over tracking down missing data.
        """
        self.get_non_upload_metadata()
        dataset_info = self._format_upload_reorganized_datasets()
        priority_projects_list = ", ".join(self.entity_data.get("priority_project_list", []))
        msg_data = {
            "uuid": self.entity_data.get("uuid"),
            "hubmap_id": self.entity_data.get("hubmap_id"),
            "created_by_user_displayname": self.entity_data.get("created_by_user_displayname"),
            "created_by_user_email": self.entity_data.get("created_by_user_email"),
            "priority_project_list": priority_projects_list,
            "dataset_type": self.dataset_type,
            "organ": self.organ,
        }
        return self.create_msg_str(priority_projects_list, msg_data, dataset_info)

    def create_msg_str(
        self, upload_label: str, data: dict[str, str], dataset_info: list[str]
    ) -> str:
        print_vals = (
            "\n   ".join([f"{key}: {value}" for key, value in data.items()])
            + "\n\nDatasets:\n"
            + "\n".join(dataset_info)
        )
        return f"""Priority upload ({upload_label}) reorganized:\n   {print_vals}"""
