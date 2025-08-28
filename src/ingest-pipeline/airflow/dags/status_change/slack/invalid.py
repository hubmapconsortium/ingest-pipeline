from ..status_utils import get_primary_dataset
from .base import SlackMessage


class SlackUploadInvalid(SlackMessage):
    name = "upload_invalid"

    def format(self):
        return f"""
            Upload <{self.get_globus_url}|{self.uuid}> is in Invalid state.
            {self.dataset_links}
            """


class SlackDatasetInvalid(SlackMessage):
    """
    Primary dataset is invalid.
    """

    name = "dataset_invalid"

    def format(self):
        if primary_dataset := get_primary_dataset(self.entity_data):
            # Just in case any derived datasets make it here.
            child_uuid = self.uuid
            self.uuid = primary_dataset
            return f"""
                Derived dataset <{self.get_globus_url(child_uuid)}|{child_uuid}> is in Error state.
                Primary dataset: <{self.get_globus_url()}|{self.uuid}>
                {self.dataset_links}
            """
        return f"""
            Dataset <{self.get_globus_url}|{self.uuid}> is in Invalid state.
            {self.dataset_links}
            """
