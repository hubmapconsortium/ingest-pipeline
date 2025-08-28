from ..status_utils import get_primary_dataset
from .base import SlackMessage


class SlackUploadError(SlackMessage):
    name = "upload_error"

    def format(self):
        return f"""
            Upload <{self.get_globus_url}|{self.uuid}> is in Error state.
            {self.dataset_links}
            """


class SlackDatasetError(SlackMessage):
    """
    Error occurred during pipeline processing.
    """

    name = "dataset_error"

    def format(self):
        child_uuid = self.uuid
        primary_dataset = get_primary_dataset(self.entity_data, self.token)
        self.uuid = primary_dataset
        return f"""
            Derived dataset <{self.get_globus_url(child_uuid)}|{child_uuid}> is in Error state.
            Primary dataset: <{self.get_globus_url()}|{self.uuid}>
            {self.dataset_links}
            """


class SlackDatasetErrorPrimary(SlackDatasetError):
    """
    Just in case any non-derived datasets make it here.
    """

    name = "dataset_error_primary"

    @classmethod
    def test(cls, entity_data, token):
        if not get_primary_dataset(entity_data, token):
            return True
        return False

    def format(self):
        return f"""
            Dataset <{self.get_globus_url}|{self.uuid}> is in Error state.
            {self.dataset_links}
            """
