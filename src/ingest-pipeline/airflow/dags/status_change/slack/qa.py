from status_change.status_utils import get_entity_ingest_url, get_primary_dataset

from .base import SlackMessage


class SlackDatasetQA(SlackMessage):
    name = "dataset_qa"

    def format(self):
        return [f"Dataset {self.uuid} has reached QA!", self.entity_links_str]


class SlackDatasetQADerived(SlackMessage):
    name = "dataset_qa_derived"

    @classmethod
    def test(cls, entity_data, token, derived):
        if derived and entity_data.get("uuid"):
            return True
        elif get_primary_dataset(entity_data, token):
            return True
        return False

    def format(self):
        message = [f"Derived dataset {self.uuid} has reached QA!"]
        if self.primary_dataset_info:
            message.append(
                f"Primary dataset: {get_entity_ingest_url(self.primary_dataset_info)}|{self.primary_dataset_info.get(self.entity_id_str)}."
            )
        message.append(self.entity_links_str)
        return message
