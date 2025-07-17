from status_change.status_utils import get_globus_url

from .base import SlackMessage


class SlackDatasetError(SlackMessage):
    name = "dataset_error"

    def format(self):
        # Currently no generalized data
        raise NotImplementedError


class SlackDatasetErrorPipeline(SlackMessage):
    # TODO: get channel
    name = "dataset_error_pipeline"

    # TODO: is it possible to identify failed pipeline by name?

    @classmethod
    def test(cls, entity_data) -> bool:
        # How do you know if something came from a pipeline?
        # "creation_action": "Central Process"?
        # "pipeline_message": "the process ran"?
        # does something need to be passed in?
        return False

    def format(self):
        return f"Dataset <{get_globus_url}|{self.uuid}> failed in pipeline."
