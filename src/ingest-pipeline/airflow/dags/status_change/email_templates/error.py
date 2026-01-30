from status_change.status_utils import get_entity_ingest_url, split_error_counts

from .base import EmailTemplate


class ErrorStatusEmail(EmailTemplate):

    def format(self) -> tuple[str, str]:
        if self.data.derived:
            subj = f"Internal error for derived dataset {self.data.entity_id}"
        elif self.data.processing_pipeline:
            subj = (
                f"Internal error while processing pipeline primary dataset {self.data.entity_id}"
            )
        else:
            subj = f"Internal error for {self.data.entity_type} {self.data.entity_id}"
        msg = self.format_msg()
        return subj, msg

    def format_msg(self) -> str:
        msg = [
            f"{self.data.project.value[1]} ID: {self.data.entity_id}",
            f"UUID: {self.data.uuid}",
            f"Entity type: {self.data.entity_type}",
            f"Status: {self.data.status.titlecase}",
            f"Group: {self.data.entity_data.get('group_name')}",
            f"Primary contact: {self.data.primary_contact}",
            f"Ingest page: {get_entity_ingest_url(self.data.entity_data)}",
        ]
        if self.data.run_id:
            msg.append(f"Run ID: {self.data.run_id}")
        if self.data.log_directory_path:
            msg.append(f"Log file: {self.data.log_directory_path}")
        if self.data.error_counts:
            msg.extend(["", "Error:"])
            msg.extend(split_error_counts(self.data.error_counts, no_bullets=True))
        return self.stringify(msg)
