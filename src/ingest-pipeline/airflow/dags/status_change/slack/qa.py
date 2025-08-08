from status_change.status_utils import get_globus_url

from .base import SlackMessage


class SlackDatasetQA(SlackMessage):
    name = "dataset_qa"

    def format(self):
        msg = f"""
        Dataset <{get_globus_url}|{self.uuid}> has reached QA!
        """
        return msg
