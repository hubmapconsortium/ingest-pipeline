import logging
from typing import Optional

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
from .slack.reorganized import (
    SlackUploadReorganized,
    SlackUploadReorganizedNoDatasets,
    SlackUploadReorganizedPriority,
)
from .status_utils import (
    EntityUpdateException,
    MessageManager,
    Statuses,
    get_env,
    slack_channels_testing,
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
        status: Statuses,
        uuid: str,
        token: str,
        messages: Optional[dict] = None,
        run_id: str = "",
        *args,
        **kwargs,
    ):
        super().__init__(status, uuid, token, messages, run_id, args, kwargs)
        self.message_class = self.get_message_class(status)
        if not self.message_class:
            logging.info(
                f"Status {status.value} does not have any Slack messaging rules; no message will be sent."
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
            "subclasses": [SlackUploadReorganizedPriority, SlackUploadReorganizedNoDatasets],
        },
    }

    @property
    def is_valid_for_status(self):
        return bool(self.message_class)

    def get_message_class(self, msg_type: Statuses) -> Optional[SlackMessage]:
        relevant_classes = self.status_to_class.get(msg_type)
        if not relevant_classes:
            return
        # Re-request entity data as previous message managers may have altered it
        for subclass in relevant_classes.get("subclasses", []):
            if subclass.test(self.entity_data, self.token):
                return subclass(self.uuid, self.token)
        if main_class := relevant_classes["main_class"]:
            return main_class(self.uuid, self.token)

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
        try:
            env = get_env()
        except Exception as e:
            env = "dev"
            logging.info(f"Error retrieving env, defaulting to DEV. {e}")
        if env == "prod":
            channel = self.message_class.channel
        else:
            channel = slack_channels_testing.get(self.message_class.name)
            logging.info(
                f"Non-prod environment, switching channel from {self.message_class.channel} to {channel}."
            )
            if not channel:
                channel = slack_channels_testing.get("base", "")
        if not channel:
            channel = SlackMessage.get_channel()
            logging.info(
                f"No channel found for message class {self.message_class.name} on {env}, using default channel."
            )
        return channel
