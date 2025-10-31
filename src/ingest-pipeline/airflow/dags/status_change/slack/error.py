from ..status_utils import EntityUpdateException
from .base import SlackMessage


class SlackUploadError(SlackMessage):
    name = "upload_error"

    def format(self):
        return [f"Upload {self.uuid_and_entity_id_str} is in Error state.", self.entity_links_str]


class SlackDatasetError(SlackMessage):
    name = "dataset_error"

    def format(self):
        return [f"Dataset {self.uuid_and_entity_id_str} is in Error state.", self.entity_links_str]


class SlackDatasetErrorDerived(SlackMessage):
    """
    Error occurred during pipeline processing.
    """

    name = "dataset_error_derived"

    # TODO: test
    @classmethod
    def test(cls, entity_data, token, primary_dataset={}) -> bool:
        del entity_data, token
        if primary_dataset:
            return True
        return False

    # TODO: test
    def format(self):
        if not self.primary_dataset:
            raise EntityUpdateException(f"Could not locate primary dataset uuid for {self.uuid}.")
        # No derived UUID, message for primary
        if not self.uuid:
            return [
                f"Error processing primary dataset {self.uuid}.",
                *self.entity_links_str,
            ]
        # We have both derived and primary UUID, derived dataset was created
        derived_uuid = self.uuid
        derived_entity_data = self.entity_data
        self.uuid = self.primary_dataset.get("uuid", "")
        self.entity_data = self.primary_dataset
        return [
            f"Derived dataset {derived_uuid} | {derived_entity_data.get(self.entity_id_str)} is in Error state.",
            f"Primary dataset: {self.uuid_and_entity_id_str}",
            *self.entity_links_str,
        ]
