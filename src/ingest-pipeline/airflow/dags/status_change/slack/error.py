from status_change.status_utils import get_is_derived

from .base import SlackMessage


class SlackUploadError(SlackMessage):
    name = "upload_error"

    def format(self):
        return [f"Upload {self.entity_id} | {self.uuid} is in Error state.", *self.entity_links]


class SlackDatasetError(SlackMessage):
    name = "dataset_error"

    def format(self):
        return [f"Dataset {self.entity_id} | {self.uuid} is in Error state.", *self.entity_links]


class SlackDatasetErrorProcessing(SlackMessage):
    """
    Error in processing pipeline. If a pipeline_name
    was passed in, figure out whether this is a derived
    dataset or not and message accordingly.
    """

    name = "dataset_error_processing"

    def format(self):
        derived = get_is_derived(self.entity_data)
        if derived:
            message = [f"Derived dataset {self.entity_id} | {self.uuid} is in Error state."]
        else:
            message = [f"Pipeline processing failed for {self.entity_id} | {self.uuid}."]
        message.extend(self.entity_links)
        if derived and self.primary_dataset_info:
            message.append(f"Primary dataset: {self.create_primary_link()}.")
        return message

    @classmethod
    def test(cls, entity_data, **kwargs):
        if kwargs.get("processing_pipeline"):
            return True
        return get_is_derived(entity_data)
