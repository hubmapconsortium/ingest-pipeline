from ..status_utils import EntityUpdateException, get_entity_ingest_url
from .base import SlackMessage


class SlackUploadError(SlackMessage):
    name = "upload_error"

    def format(self):
        return [f"Upload {self.uuid_and_entity_id_str} is in Error state.", self.entity_links_str]


class SlackDatasetError(SlackMessage):
    name = "dataset_error"

    def format(self):
        return [f"Dataset {self.uuid_and_entity_id_str} is in Error state.", self.entity_links_str]


class SlackDatasetErrorDerivedCreated(SlackMessage):
    """
    Error occurred during pipeline processing after
    derived dataset was created.
    """

    name = "dataset_error_derived_created"

    @classmethod
    def test(cls, entity_data, token, handle_derived=False, derived_dataset=False) -> bool:
        del entity_data, token
        if handle_derived and derived_dataset:
            return True
        return False

    def format(self):
        if not self.derived_dataset:
            raise EntityUpdateException(f"Could not locate derived dataset uuid {self.uuid}.")
        return [
            f"Derived dataset {self.derived_dataset.get('uuid')} | {self.derived_dataset.get(self.entity_id_str)} is in Error state.",
            f"Primary dataset: {self.uuid_and_entity_id_str}",
            f"<{get_entity_ingest_url(self.derived_dataset)}|View on Ingest UI.>",
        ]


class SlackDatasetErrorDerivedNotCreated(SlackMessage):
    """
    Error occurred during pipeline processing before
    derived dataset creation.
    """

    name = "dataset_error_derived_not_created"

    @classmethod
    def test(cls, entity_data, token, handle_derived=False, derived_dataset=False) -> bool:
        del entity_data, token
        if handle_derived and not derived_dataset:
            return True
        return False

    def format(self):
        return [
            f"Error processing primary dataset {self.uuid_and_entity_id_str}.",
            *self.entity_links_str,
        ]
