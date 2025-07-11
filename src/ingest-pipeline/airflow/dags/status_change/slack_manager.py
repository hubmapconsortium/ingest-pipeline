import json
import logging

from airflow.providers.http.hooks.http import HttpHook

from .slack.qa import SlackQA
from .slack.reorganized import SlackPriorityReorganized, SlackReorganized
from .status_utils import (
    EntityUpdateException,
    Statuses,
)


class SlackManager:
    """
    Allows StatusChanger to remain agnostic about Slack requirements for different statuses
    and the nuances of their (potential) subclasses; manager handles traffic direction.
    The relevant message class is composed into this manager based on status and result
    of `test` for any subclasses.
    Use:
        msg_and_channel_dict = SlackManager(Statuses.<status>, <uuid>, <token>).send()
    """

    def __init__(self, status: Statuses, uuid: str, token: str):
        self.uuid = uuid
        self.token = token
        self.get_message_class(status)
        if not self.message_class:
            logging.info(
                f"Status {status} does not have any Slack messaging rules; no message will be sent."
            )

    @property
    def status_to_class(self):
        """
        This is the source of truth for what classes should be used for a given status.
        Format:
            Statuses.STATUS: {"main_class": <class_name>, "subclasses": [<class_name>]}
        """
        return {
            Statuses.UPLOAD_REORGANIZED: {
                "main_class": SlackReorganized,
                "subclasses": [SlackPriorityReorganized],
            },
            Statuses.DATASET_QA: {
                "main_class": SlackQA,
                "subclasses": [],
            },
            # TODO: dataset errors--processing vs. reorg?
        }

    def get_message_class(self, msg_type: Statuses):
        relevant_classes = self.status_to_class.get(msg_type)
        if not relevant_classes:
            self.message_class = None
            return
        self.message_class = relevant_classes["main_class"](self.uuid, self.token)
        for subclass in relevant_classes.get("subclasses", []):
            if subclass.test(self.message_class.entity_data):
                self.message_class = subclass(self.uuid, self.token)
                break

    def send(self):
        if not self.message_class:
            return
        message = self.message_class.format()
        channel = self.message_class.channel
        if not (message and channel):
            raise EntityUpdateException(
                f"Request to send Slack message missing message text (submitted: '{message}')"
                f" or target channel (submitted: '{channel}')."
            )

    def post_to_notify(self):
        http_hook = HttpHook("POST", http_conn_id="ingest_api_connection")
        payload = json.dumps({"message": message, "channel": channel})
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        response = http_hook.run("/notify", payload, headers)
        response.raise_for_status()
