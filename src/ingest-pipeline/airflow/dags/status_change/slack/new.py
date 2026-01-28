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
        message.extend(self.entity_links)
        if self.primary_dataset_info:
            message.append(f"Primary dataset: {self.create_primary_link()}.")

        return message

    @classmethod
    def test(cls, entity_data, token, **kwargs):
        del token, kwargs
        return get_is_derived(entity_data)
