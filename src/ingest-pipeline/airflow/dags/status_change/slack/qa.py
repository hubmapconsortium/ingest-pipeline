from ..status_utils import EntityUpdateException
from .base import SlackMessage


class SlackDatasetQA(SlackMessage):
    name = "dataset_qa"

    def format(self):
        return [f"Dataset {self.uuid} has reached QA!", self.entity_links_str]


class SlackDatasetQADerived(SlackMessage):
    name = "dataset_qa_derived"

    @classmethod
    def test(cls, entity_data, token, primary_dataset={}) -> bool:
        del entity_data, token
        if primary_dataset:
            return True
        return False

    def format(self):
        # TODO: test
        if not self.primary_dataset:
            raise EntityUpdateException(f"Could not locate primary dataset uuid for {self.uuid}.")
        return [
            f"Derived dataset {self.uuid_and_entity_id_str} has been created!",
            f"Primary dataset: {self.primary_dataset.get('uuid')} | {self.primary_dataset.get(self.entity_id_str)}>",
            f"<{self.ingest_ui_url}|View on Ingest UI.>",
        ]
