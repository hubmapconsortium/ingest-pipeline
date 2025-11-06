import logging
from typing import Optional

from status_change.status_utils import (
    Statuses,
    get_entity_ingest_url,
    get_project,
    get_submission_context,
    is_internal_error,
    log_directory_path,
    split_error_counts,
)

from airflow.configuration import conf as airflow_conf
from airflow.utils.email import send_email


class EmailManager:
    int_recipients = ["bhonick@psc.edu"]
    main_recipients = ""
    cc = ""
    subj = ""
    msg = ""
    good_statuses = [
        Statuses.DATASET_QA,
        Statuses.UPLOAD_VALID,
    ]

    def __init__(
        self,
        status: Statuses,
        uuid: str,
        token: str,
        msg: str = "",
        run_id: str = "",
        *args,
        **kwargs,
    ):
        del args, kwargs
        self.uuid = uuid
        self.token = token
        self.status = status
        self.addtl_msg = str(msg) if msg else None
        self.run_id = run_id
        self.entity_data = get_submission_context(self.token, self.uuid)
        self.entity_type = self.entity_data.get("entity_type", "").title()
        self.project = get_project()
        self.is_internal_error = is_internal_error(self.entity_data)
        self.entity_id = self.entity_data.get(f"{get_project().value[0]}_id")
        self.primary_contact = self.entity_data.get("created_by_user_email", "")
        self.get_message_content()
        self.is_valid_for_status = bool(self.subj and self.msg)

    def update(self):
        if not (self.subj and self.msg):
            logging.error(
                f"""
            Status is valid for EmailManager but missing full message content.
            Subject: {self.subj}.
            Message: {self.msg}
            Exiting without sending.
            """
            )
            return
        self.get_recipients()
        self.send_email()

    def get_message_content(self) -> Optional[tuple[str, str]]:
        if self.is_internal_error:  # error status or bad content in validation_message
            subj, msg = self.internal_error_format()
        elif (
            self.status in self.good_statuses or self.reorg_status_with_child_datasets()
        ):  # good status or reorg with child datasets
            subj, msg = self.generic_good_status_format()
        elif self.status in [
            Statuses.DATASET_INVALID,
            Statuses.UPLOAD_INVALID,
        ]:  # actually invalid
            subj, msg = self.get_ext_invalid_format()
        else:
            return
        self.subj = subj
        if self.addtl_msg and self.addtl_msg != self.entity_data.get("error_message"):
            msg.append(self.addtl_msg)
        msg.extend(
            [
                "",
                "This email address is not monitored. Please email ingest@hubmapconsortium.org with any questions about your data submission.",
            ]
        )
        self.msg = msg

    def send_email(self):
        assert self.subj and self.msg
        msg_str = "<br>".join([line.lstrip() for line in self.msg])
        logging.info(
            f"""
        Sending email
            Subject: {self.subj}
            Message: {msg_str}
            """
        )
        send_email(self.main_recipients, self.subj, msg_str, cc=self.cc)

    def get_recipients(self):
        conf_dict = airflow_conf.as_dict()
        # Allows for setting defaults at the config level that override class defaults, e.g. for testing
        if int_recipients := conf_dict.get("email_notifications", {}).get("int_recipients"):
            cleaned_int_recipients = [str(address) for address in [int_recipients]]
            self.int_recipients = cleaned_int_recipients
        if main_recipient := conf_dict.get("email_notifications", {}).get("main"):
            self.primary_contact = main_recipient

        if self.is_internal_error:
            self.main_recipients = ", ".join(self.int_recipients)
        else:
            # TODO: turning off any ext emails for testing
            # self.main_recipients = self.primary_contact
            self.main_recipients = ", ".join(self.int_recipients)
            self.cc = ", ".join(self.int_recipients)

    #############
    # Templates #
    #############

    def generic_good_status_format(self) -> tuple[str, list]:
        subj = f"{self.entity_type} {self.entity_id} has successfully reached status {self.status.titlecase}!"
        msg = [f"View ingest record: {get_entity_ingest_url(self.entity_data)}"]
        return subj, msg

    def internal_error_format(self) -> tuple[str, list]:
        subj = f"Internal error for {self.entity_type} {self.entity_id}"
        msg = [
            f"{self.project.value[1]} ID: {self.entity_id}",
            f"UUID: {self.uuid}",
            f"Entity type: {self.entity_type}",
            f"Status: {self.status.titlecase}",
            f"Group: {self.entity_data.get('group_name')}",
            f"Primary contact: {self.primary_contact}",
            f"Ingest page: {get_entity_ingest_url(self.entity_data)}",
            f"Log file: {log_directory_path(self.run_id)}",
        ]
        if error_message := self.entity_data.get("error_message"):
            msg.extend(["", "Error:"])
            msg.extend(split_error_counts(error_message, no_bullets=True))
        return subj, msg

    def get_ext_invalid_format(self) -> tuple[str, list]:
        subj = f"{self.entity_type} {self.entity_id} is invalid"
        msg = [
            f"{self.entity_type} {self.entity_id} has failed validation. The validation process starts by performing initial checks such as ensuring that files open correctly or that the dataset type is recognized. Next, metadata TSVs and directory structures are validated. If those checks pass, then certain individual file types (such as FASTQ and OME.TIFF files) are validated.",
            "",
            f"This upload failed at the {self.classified_errors} validation stage.",
            "",
            f"You can see the errors for this {self.entity_type.lower()} at the bottom of this email or on the Ingest page: {get_entity_ingest_url(self.entity_data)}",
            "",
            f"If you have questions about your {self.entity_type.lower()}, please schedule an appointment with Data Curator Brendan Honick: https://calendly.com/bhonick-psc/.",
        ]
        if self.entity_data.get("validation_message", ""):
            msg.extend(["", "", "", "Validation errors:", "", *self.formatted_validation_report])
        return subj, msg

    ##########
    # Format #
    ##########

    @property
    def classified_errors(self) -> str:
        classified_errors = []
        error_str = self.entity_data.get("error_message", "").lower()
        if "preflight" in error_str:
            return "initial check"
        classification = {
            "directory": ["directory"],
            "metadata": [
                "local",
                "spreadsheet",
                "url",
                "constraint",
                "antibodies",
                "contributors",
                "reference",
            ],
            "file": ["file"],
        }
        for classification, error_type in classification.items():
            for error in error_type:
                if error in error_str:
                    classified_errors.append(classification)
        classified_errors = list(set(classified_errors))
        if len(classified_errors) == 1:
            return classified_errors[0]
        if classified_errors:
            classified_errors.sort()
            return ", ".join(classified_errors[:-1]) + " and " + classified_errors[-1]
        return ""

    @property
    def formatted_validation_report(self) -> list:
        report = self.entity_data.get("validation_message", "")
        list_report = [line.strip() for line in report.split("\n")]
        formatted_list_report = []
        incomplete_line = None
        for line in list_report:
            if incomplete_line:
                line = incomplete_line + line[1:]
                incomplete_line = None
            if line.endswith("\\"):
                incomplete_line = line[:-1]
                continue
            if line.startswith("-"):
                formatted_list_report.append(f"     {line}")
            elif not line:
                continue
            else:
                formatted_list_report.append(line)
        return formatted_list_report

    #########
    # Tests #
    #########

    def reorg_status_with_child_datasets(self):
        if self.status == Statuses.UPLOAD_REORGANIZED and self.entity_data.get("datasets"):
            logging.info(
                "Reorganized upload does not have child datasets (DAG may still be running); not sending email."
            )
            return True  # only want to send good email if reorg status AND has child datasets
        return False
