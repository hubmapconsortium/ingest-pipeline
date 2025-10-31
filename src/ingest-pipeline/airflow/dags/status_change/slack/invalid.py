from .base import SlackMessage


class SlackUploadInvalid(SlackMessage):
    name = "upload_invalid"

    def format(self):
        return [
            f"Upload {self.uuid_and_entity_id_str} is in Invalid state.",
            self.entity_links_str,
        ]


class SlackDatasetInvalid(SlackMessage):
    """
    Dataset is invalid.
    """

    name = "dataset_invalid"

    def format(self):
        return [
            f"Dataset {self.uuid_and_entity_id_str} is in Invalid state.",
            self.entity_links_str,
        ]
