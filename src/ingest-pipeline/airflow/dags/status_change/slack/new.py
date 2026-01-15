from status_change.slack.base import SlackMessage
from status_change.status_utils import get_is_derived


class SlackDatasetNew(SlackMessage):
    """
    Should just pass and do nothing.
    """

    name = "dataset_new"


class SlackDatasetNewDerived(SlackMessage):
    name = "dataset_new_derived"

    def format(self):
        message = [f"Derived dataset {self.uuid} has been created."]
        if self.primary_dataset_info:
            message.append(f"Primary dataset: {self.create_primary_link()}.")
        message.append(self.entity_links_str)
        return message

    @classmethod
    def test(cls, entity_data, token, **kwargs):
        return get_is_derived(entity_data, token, **kwargs)
