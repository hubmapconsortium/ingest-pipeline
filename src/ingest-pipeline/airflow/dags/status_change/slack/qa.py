from ..status_utils import EntityUpdateException, get_entity_ingest_url
from .base import SlackMessage


class SlackDatasetQA(SlackMessage):
    name = "dataset_qa"

    def format(self):
        return [f"Dataset {self.uuid} has reached QA!", self.entity_links_str]


class SlackDatasetQADerived(SlackMessage):
    name = "dataset_qa_derived"

    @classmethod
    def test(cls, entity_data, token, handle_derived=False, derived_dataset=False) -> bool:
        del entity_data, token
        if handle_derived and derived_dataset:
            return True
        return False

    def format(self):
        if not self.derived_dataset:
            raise EntityUpdateException(f"Could not locate derived dataset for {self.uuid}.")
        return [
            f"Derived dataset {self.derived_dataset.get('uuid')} | {self.derived_dataset.get(self.entity_id_str)} has been created!",
            f"Primary dataset: {self.uuid} | {self.entity_data.get(self.entity_id_str)}",
            f"<{get_entity_ingest_url(self.derived_dataset)}|View on Ingest UI.>",
        ]
