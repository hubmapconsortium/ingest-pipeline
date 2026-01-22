from status_change.status_utils import (
    get_is_derived,
    get_primary_dataset,
)

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
        message = [f"Derived dataset {self.uuid} is in Error state.", self.entity_links_str]
        if self.primary_dataset_info:
            message.append(f"Primary dataset: {self.create_primary_link()}.")
        return message

    @classmethod
    def test(cls, entity_data, token, **kwargs):
        del token
        return get_is_derived(entity_data, **kwargs)


class SlackDatasetErrorPrimaryPipeline(SlackMessage):
    """
    Error occurred in pipeline processing before derived
    dataset created.
    """

    name = "dataset_error_primary_pipeline"

    def format(self):
        message = [
            f"Error while processing primary dataset <{self.ingest_ui_url}|{self.uuid}>.",
            f"Error message: {self.entity_data['pipeline_message']}",
        ]
        if self.run_id:
            message.append(f"Run: {self.run_id}.")
        return message

    @classmethod
    def test(cls, entity_data, token, **kwargs):
        del token
        # Weed out derived datasets
        if get_is_derived(entity_data, **kwargs):
            return False
        # Only a primary if there is a processing pipeline
        if kwargs.get("processing_pipeline"):
            return True
        return False
