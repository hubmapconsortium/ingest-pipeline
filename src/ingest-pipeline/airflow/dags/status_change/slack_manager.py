import logging
from typing import Optional, Type

from .slack.base import SlackMessage
from .slack.error import (  # SlackDatasetErrorDerived,; SlackDatasetErrorPrimary,
    SlackDatasetError,
    SlackUploadError,
)
from .slack.invalid import (  # SlackDatasetInvalidDerived,
    SlackDatasetInvalid,
    SlackUploadInvalid,
)
from .slack.qa import SlackDatasetQA
from .slack.reorganized import SlackUploadReorganized, SlackUploadReorganizedPriority
from .status_utils import (
    EntityUpdateException,
    Statuses,
    get_submission_context,
    post_to_slack_notify,
)


class SlackManager:
    """
    Allows StatusChanger to remain agnostic about Slack requirements for different statuses
    and the nuances of their (potential) subclasses; manager handles traffic direction.
    The relevant message class is composed into this manager based on status and result
    of `test` for any subclasses.
    Use:
        msg_and_channel_dict = SlackManager(Statuses.<status>, <uuid>, <token>).update()
    """

    def __init__(self, status: Statuses, uuid: str, token: str, *args, **kwargs):
        self.uuid = uuid
        self.token = token
        self.message_class = self.get_message_class(status)
        if not self.message_class:
            logging.info(
                f"Status {status.value} does not have any Slack messaging rules; no message will be sent."
            )
        self.is_valid_for_status = bool(self.message_class)

    @property
    def status_to_class(self):
        """
        This is the source of truth for what classes should be used for a given status.
        Format:
            Statuses.STATUS: {"main_class": <class_name>, "subclasses": [<class_name>]}
        """
        return {
            Statuses.DATASET_ERROR: {
                "main_class": SlackDatasetError,
                "subclasses": [],
                # "subclasses": [SlackDatasetErrorPrimary, SlackDatasetErrorDerived],
            },
            Statuses.DATASET_INVALID: {
                "main_class": SlackDatasetInvalid,
                "subclasses": [],
                # "subclasses": [SlackDatasetInvalidDerived],
            },
            Statuses.DATASET_QA: {
                "main_class": SlackDatasetQA,
                "subclasses": [],
            },
            Statuses.UPLOAD_ERROR: {
                "main_class": SlackUploadError,
                "subclasses": [],
            },
            Statuses.UPLOAD_INVALID: {
                "main_class": SlackUploadInvalid,
                "subclasses": [],
            },
            Statuses.UPLOAD_REORGANIZED: {
                "main_class": SlackUploadReorganized,
                "subclasses": [SlackUploadReorganizedPriority],
            },
        }

    def get_message_class(self, msg_type: Statuses) -> Optional[SlackMessage]:
        relevant_classes = self.status_to_class.get(msg_type)
        if not relevant_classes:
            return
        entity_data = get_submission_context(self.token, self.uuid)
        for subclass in relevant_classes.get("subclasses", []):
            if subclass.test(entity_data, self.token):
                return subclass(self.uuid, self.token, entity_data)
        if main_class := relevant_classes["main_class"]:
            return main_class(self.uuid, self.token, entity_data)

    def update(self):
        if not self.message_class:
            return
        message = self.message_class.format()
        channel = str(self.message_class.channel)
        logging.info(f"Sending message from {self.message_class.name}...")
        if message and channel:
            post_to_slack_notify(self.token, message, channel)
        elif not message:
            raise EntityUpdateException(f"Request to send Slack message missing message text.")
        elif not channel:
            # This is likely a config issue, let's soft fail here
            logging.info(
                f"Request to send Slack message missing target channel. No Slack message will be sent."
            )
