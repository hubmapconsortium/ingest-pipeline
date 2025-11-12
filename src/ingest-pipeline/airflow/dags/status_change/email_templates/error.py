from status_change.status_utils import get_entity_ingest_url, split_error_counts

from .base import EmailTemplate


class ErrorStatusEmail(EmailTemplate):

    def format(self) -> tuple[str, str]:
        subj = f"Internal error for {self.data.entity_type} {self.data.entity_id}"
        msg = [
            f"{self.data.project.value[1]} ID: {self.data.entity_id}",
            f"UUID: {self.data.uuid}",
            f"Entity type: {self.data.entity_type}",
            f"Status: {self.data.status.titlecase}",
            f"Group: {self.data.entity_data.get('group_name')}",
            f"Primary contact: {self.data.primary_contact}",
            f"Ingest page: {get_entity_ingest_url(self.data.entity_data)}",
            f"Log file: {self.data.log_directory_path}",
        ]
        if self.data.error_counts:
            msg.extend(["", "Error:"])
            msg.extend(split_error_counts(self.data.error_counts, no_bullets=True))
        return subj, self.stringify(msg)
