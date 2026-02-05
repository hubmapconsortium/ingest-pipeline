from status_change.status_utils import get_is_derived

from .base import SlackMessage


class SlackDatasetQA(SlackMessage):
    name = "dataset_qa"

    def format(self):
        return [f"Dataset {self.entity_id} | {self.uuid} has reached QA!", *self.entity_links]


class SlackDatasetQADerived(SlackMessage):
    name = "dataset_qa_derived"

    @classmethod
    def test(cls, entity_data, **kwargs):
        del kwargs
        return get_is_derived(entity_data)

    def format(self):
        message = [f"Derived dataset {self.entity_id} | {self.uuid} has reached QA!"]
        if self.primary_dataset_info:
            message.append(f"Primary dataset: {self.create_primary_link()}.")
        message.extend(self.entity_links)
        return message
