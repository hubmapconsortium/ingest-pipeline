from status_change.status_utils import get_entity_ingest_url

from .base import EmailTemplate


class GenericGoodStatusEmail(EmailTemplate):

    def format(self) -> tuple[str, str]:
        subj = f"{self.entity_type} {self.entity_id} has successfully reached status {self.status.titlecase}!"
        msg = [
            f"View ingest record: {get_entity_ingest_url(self.data)}",
            *self.footer,
        ]
        return subj, self.stringify(msg)
