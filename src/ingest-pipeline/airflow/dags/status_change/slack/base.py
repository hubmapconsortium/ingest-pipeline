from status_change.status_utils import get_submission_context

from airflow.configuration import conf as airflow_conf


class SlackMessage:
    # Name should match what's in airflow_conf.slack_channels
    name = "base"

    def __init__(self, uuid: str, token: str):
        self.uuid = uuid
        self.token = token
        self.channel = airflow_conf.as_dict().get("slack_channels", {}).get(self.name.upper())
        self.entity_data = get_submission_context(token, uuid)

    @classmethod
    def test(cls, entity_data: dict) -> bool:
        """
        If there are special case subclasses for a given status, their
        test() methods will be called to determine which subclass to use.
        """
        return True

    def format(self) -> str:
        raise NotImplementedError
