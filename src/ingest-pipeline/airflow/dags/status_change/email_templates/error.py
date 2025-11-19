from status_change.status_utils import get_entity_ingest_url, split_error_counts

from .base import EmailTemplate


class ErrorStatusEmail(EmailTemplate):

    def format(self) -> tuple[str, str]:
        subj = f"Internal error for {self.data.entity_type} {self.data.entity_id}"
        msg = [
            f"{self.project.value[1]} ID: {self.entity_id}",
            f"UUID: {self.uuid}",
            f"Entity type: {self.entity_type}",
            f"Status: {self.status.titlecase}",
            f"Group: {self.data.get('group_name')}",
            f"Primary contact: {self.data.get('primary_contact')}",
            f"Ingest page: {get_entity_ingest_url(self.data)}",
            f"Log file: {self.log_directory_path}",
        ]
        if self.error_counts:
            msg.extend(["", "Error:"])
            msg.extend(split_error_counts(self.data.error_counts, no_bullets=True))
        return subj, self.stringify(msg)
