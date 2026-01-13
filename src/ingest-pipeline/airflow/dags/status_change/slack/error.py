from status_change.status_utils import get_primary_dataset

from .base import SlackMessage


class SlackUploadError(SlackMessage):
    name = "upload_error"

    def format(self):
        return [f"Upload {self.uuid} is in Error state.", self.entity_links_str]


class SlackDatasetError(SlackMessage):
    name = "dataset_error"

    def format(self):
        return [f"Dataset {self.uuid} is in Error state.", self.entity_links_str]


class SlackDatasetErrorDerived(SlackMessage):
    """
    Derived dataset has been created but is in error state.
    """

    name = "dataset_error_derived"

    def format(self):
        return [f"Derived dataset {self.uuid} is in Error state.", self.entity_links_str]

    @classmethod
    def test(cls, entity_data, token, derived):
        if derived and entity_data.get("uuid"):
            return True
        elif get_primary_dataset(entity_data, token):
            return True
        return False
