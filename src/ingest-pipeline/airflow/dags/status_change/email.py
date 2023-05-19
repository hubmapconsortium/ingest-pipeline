import logging
from dataclasses import dataclass

from airflow.utils.email import send_email


# Need to create EmailData object when returning template data
@dataclass
class EmailData:
    internal_subject: str
    internal_message: str
    external_subject: str
    external_message: str
    internal_recipients: list[str]
    external_recipients: list[str]


def send_notification_email(email_data: EmailData):
    try:
        if email_data.internal_subject and email_data.internal_message:
            send_email(
                to=email_data.internal_recipients,
                subject=email_data.internal_subject,
                html_content=email_data.internal_message,
            )
            logging.info(
                f"""Internal notification email sent to {email_data.internal_recipients}.
                Subject: {email_data.internal_subject}
                Message: {email_data.internal_message}
                """
            )
        if email_data.external_subject and email_data.external_message:
            send_email(
                to=email_data.external_recipients,
                subject=email_data.external_subject,
                html_content=email_data.external_message,
            )
            logging.info(
                f"""
                External notification email sent to {email_data.external_recipients}.
                Subject: {email_data.external_subject}
                Message: {email_data.external_message}
                """
            )
    except Exception as e:
        logging.info(
            f"""
            Error encountered while sending notification.
            Template data provided: {email_data}
            Error: {e}
            """
        )
        raise
