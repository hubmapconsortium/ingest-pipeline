import logging
from typing import Optional

from status_change.slack.new import SlackDatasetNew, SlackDatasetNewDerived

from .slack.base import SlackMessage
from .slack.error import (
    SlackDatasetError,
    SlackDatasetErrorProcessing,
    SlackUploadError,
)
from .slack.invalid import (
    SlackDatasetInvalid,
    SlackUploadInvalid,
)
from .slack.qa import SlackDatasetQA, SlackDatasetQADerived
from .slack.reorganized import (
    SlackUploadReorganized,
    SlackUploadReorganizedNoDatasets,
    SlackUploadReorganizedPriority,
)
from .status_utils import (
    EntityUpdateException,
    MessageManager,
    Statuses,
    env_appropriate_slack_channel,
    split_error_counts,
)


class SlackManager(MessageManager):
    """
    Allows StatusChanger to remain agnostic about Slack requirements for different statuses
    and the nuances of their (potential) subclasses; manager handles traffic direction.
    The relevant message class is composed into this manager based on status and result
    of `test` for any subclasses.
    Use:
        msg_and_channel_dict = SlackManager(Statuses.<status>, <uuid>, <token>).update()
    """

    def __init__(
        self,
        status,
        uuid,
        token,
        messages=None,
        *args,
        **kwargs,
    ):
        super().__init__(status, uuid, token, messages, *args, **kwargs)
        self.message_class = self.get_message_class(self.status)
        if not self.message_class:
            logging.info(
                f"Status {self.status.value} does not have any Slack messaging rules; no message will be sent."
            )

    """
    This is the source of truth for what classes should be used for a given status.
    Format:
        Statuses.STATUS: {"main_class": <class_name>, "subclasses": [<class_name>]}
    """
    status_to_class = {
        Statuses.DATASET_ERROR: {
            "main_class": SlackDatasetError,
            "subclasses": [],
            "subclasses": [SlackDatasetErrorProcessing],
        },
        Statuses.DATASET_INVALID: {
            "main_class": SlackDatasetInvalid,
            "subclasses": [],
        },
        Statuses.DATASET_NEW: {
            "main_class": SlackDatasetNew,
            "subclasses": [SlackDatasetNewDerived],
        },
        Statuses.DATASET_QA: {
            "main_class": SlackDatasetQA,
            "subclasses": [SlackDatasetQADerived],
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
            "subclasses": [SlackUploadReorganizedPriority, SlackUploadReorganizedNoDatasets],
        },
    }

    @property
    def is_valid_for_status(self):
        return bool(self.message_class)

    def get_message_class(self, msg_type: Statuses) -> Optional[SlackMessage]:
        if not (relevant_classes := self.status_to_class.get(msg_type)):
            return
        class_args = [self.uuid, self.token]
        class_kwargs = {"run_id": self.run_id, "processing_pipeline": self.processing_pipeline}
        test_kwargs = {
            "processing_pipeline": self.processing_pipeline,
        }
        for subclass in relevant_classes.get("subclasses", []):
            if subclass.test(self.entity_data, **test_kwargs):
                return subclass(*class_args, **class_kwargs)
        if main_class := relevant_classes["main_class"]:
            return main_class(*class_args, **class_kwargs)

    def update(self):
        from utils import post_to_slack_notify

        if not self.message_class:
            raise EntityUpdateException("Can't update Slack without message class, exiting.")
        message = self.get_message()
        channel = self.get_channel()
        logging.info(f"Sending message from {self.message_class.name}...")
        logging.info(f"Channel: {channel}")
        logging.info(f"Message: {message}")
        try:
            post_to_slack_notify(self.token, message, channel)
        except Exception as e:
            raise EntityUpdateException(f"No Slack message sent. Encountered error: {e}")

    def get_message(self) -> str:
        if not self.message_class:
            raise EntityUpdateException("Can't format message without message class.")
        try:
            message = self.message_class.format()
            if self.error_counts:
                message.extend(["", "Error Counts:", *split_error_counts(self.error_counts)])
        except NotImplementedError:
            raise EntityUpdateException(
                f"Message class {self.message_class.name} does not implement a format method; not sending Slack message."
            )
        if not message:
            raise EntityUpdateException(f"Request to send Slack message missing message text.")
        return "\n".join([line for line in message])

    def get_channel(self) -> str:
        if not self.message_class:
            raise EntityUpdateException("Can't retrieve channel without message class.")
        return env_appropriate_slack_channel(self.message_class.channel)
