from .base import SlackMessage


class SlackDatasetQA(SlackMessage):
    name = "dataset_qa"

    def format(self):
        msg = f"""
        Dataset {self.uuid} has reached QA!
        {self.entity_links_str}
        """
        return msg


# class SlackDatasetQADerived(SlackMessage):
#     name = "dataset_qa_derived"
#
#     @classmethod
#     def test(cls, entity_data, token) -> bool:
#         if entity_data.get("entity_type", "").lower() == "dataset":
#             for ancestor in entity_data.get("direct_ancestors", []):
#                 if ancestor.get("entity_type").lower() == "dataset":
#                     return True
#         return False
#
#     def format(self):
#         # TODO: needs bespoke links
#         parent = ""  # TODO
#         msg = f"""
#         Derived dataset {self.uuid} has been created!
#         Parent: {parent}
#         {self.entity_links_str}
#         """
#         return msg
