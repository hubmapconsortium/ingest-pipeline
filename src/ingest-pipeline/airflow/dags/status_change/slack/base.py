from typing import Optional

from status_change.status_utils import get_submission_context

from airflow.configuration import conf as airflow_conf


class SlackMessage:
    # Name should match what's in airflow_conf.slack_channels
    name = "base"

    def __init__(self, uuid: str, token: str, entity_data: Optional[dict] = None):
        self.uuid = uuid
        self.token = token
        self.channel = airflow_conf.as_dict().get("slack_channels", {}).get(self.name.upper())
        self.entity_data = entity_data if entity_data else get_submission_context(token, uuid)

    @classmethod
    def test(cls, entity_data: dict) -> bool:
        """
        If there are special case subclasses for a given status, their
        test() methods will be called to determine if the subclass applies.
        Only one should return True because the subclass test loop breaks
        after first True result.
        """
        return False

    def format(self) -> str:
        raise NotImplementedError
