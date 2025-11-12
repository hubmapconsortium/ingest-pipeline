from status_change.status_utils import get_entity_ingest_url

from .base import EmailTemplate


class InvalidStatusEmail(EmailTemplate):

    def format(self) -> tuple[str, str]:
        subj = f"{self.data.project.value[1]} {self.data.entity_type} {self.data.entity_id} is invalid"
        msg = [
            f'{self.data.project.value[1]} {self.data.entity_type} <a href="{get_entity_ingest_url(self.data.entity_data)}">{self.data.entity_id}</a> has failed validation.',
            "",
            "<b>Validation details</b>",
            "The validation process starts by checking metadata TSVs and directory structures. If those checks pass, then certain individual file types (such as FASTQ and OME.TIFF files) are validated.",
            "",
            "<b>What to do next</b>",
            f"If you have questions about your {self.data.entity_type.lower()}, please schedule an appointment with Data Curator Brendan Honick: https://calendly.com/bhonick-psc/.",
            "",
            "This email address is not monitored. If you have questions, please schedule with Brendan Honick or email ingest@hubmapconsortium.org.",
        ]
        if self.data.error_dict:
            msg.extend(
                [
                    "",
                    "The error log is included below if you would like to make updates to your submission independently; it is not required for you to do so before contacting our Data Curation Team. Please email ingest@hubmapconsortium.org if you believe you have repaired all validation errors so that we can re-validate your submission.",
                    "",
                    'If your submission has "Spreadsheet Validator Errors," please use the <a href="https://metadatavalidator.metadatacenter.org/">Metadata Spreadsheet Validator</a> tool to correct them.',
                    "",
                    "<b>Validation error log</b>",
                    *self.html_formatted_error_list,
                ]
            )
        return subj, self.stringify(msg)
