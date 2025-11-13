from status_change.status_utils import get_entity_id, get_entity_ingest_url

from .base import EmailTemplate


class ReorganizedStatusEmail(EmailTemplate):

    def format(self) -> tuple[str, str]:
        subj = f"Datasets successfully created for upload {self.data.entity_id}!"
        msg = [
            f"View ingest record: {get_entity_ingest_url(self.data.entity_data)}",
            "Datasets:",
            self.dataset_info(),
            *self.footer,
        ]
        return subj, self.stringify(msg)

    def dataset_info(self):
        datasets = self.data.entity_data.get("datasets", [])
        dataset_info = []
        for dataset in datasets:
            dataset_info.append(
                f"<a href='{get_entity_ingest_url(dataset)}'>{get_entity_id(dataset)}</a>: {dataset.get('status')}"
            )
        return "\n".join(dataset_info)
