# from ..status_utils import get_primary_dataset
from .base import SlackMessage


class SlackUploadError(SlackMessage):
    name = "upload_error"

    def format(self):
        return f"""
        Upload {self.uuid} is in Error state.
        {self.entity_links_str}
        """


class SlackDatasetError(SlackMessage):
    name = "dataset_error"

    def format(self):
        return f"""
        Dataset {self.uuid} is in Error state.
        {self.entity_links_str}
        """


# class SlackDatasetErrorDerived(SlackMessage):
#     """
#     Error occurred during pipeline processing.
#     """
#
#     name = "dataset_error_derived"
#
#     @classmethod
#     def test(cls, entity_data, token):
#         if get_primary_dataset(entity_data, token):
#             return True
#         return False
#
#     def format(self):
#         child_uuid = self.uuid
#         primary_dataset = get_primary_dataset(self.entity_data, self.token)
#         self.uuid = primary_dataset
#         return f"""
#         Derived dataset <{self.get_globus_url(child_uuid)}|{child_uuid}> is in Error state.
#         Primary dataset: <{self.get_globus_url()}|{self.uuid}>
#         {self.entity_links_str}
#         """
#
#
# class SlackDatasetErrorPrimary(SlackDatasetError):
#     """
#     Error in primary dataset (e.g. uncaught exception during scan_and_begin_processing)
#     """
#
#     name = "dataset_error_primary"
#
#     @classmethod
#     def test(cls, entity_data, token):
#         if not get_primary_dataset(entity_data, token):
#             return True
#         return False
#
#     def format(self):
#         return f"""
#         Dataset {self.uuid} is in Error state.
#         {self.entity_links_str}
#         """
