from .base import SlackMessage


class SlackUploadInvalid(SlackMessage):
    name = "upload_invalid"

    def format(self):
        return [f"Upload {self.uuid} is in Invalid state.", *self.entity_links]


class SlackDatasetInvalid(SlackMessage):
    """
    Primary dataset is invalid.
    """

    name = "dataset_invalid"

    def format(self):
        return [f"Dataset {self.uuid} is in Invalid state.", *self.entity_links]
