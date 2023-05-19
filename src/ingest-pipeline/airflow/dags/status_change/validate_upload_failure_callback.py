# from airflow.utils.email import send_email

from .failure_callback import FailureCallback
from .status_manager import Statuses

# from .failure_callback import FailureCallbackException


class ValidateUploadFailure(FailureCallback):
    # TODO: on hold until specifics of notifications are figured out
    # Should probably be importing custom exceptions rather than comparing strings
    # external_exceptions = [
    #     "ValueError",
    #     "PreflightError",
    #     "ValidatorError",
    #     "DirectoryValidationErrors",
    #     "FileNotFoundError",
    # ]

    def get_status(self):
        entity_type = self.context.get("task_instance").xcom_pull(key="entity_type")
        # TODO: logic of choosing ERROR vs. INVALID if report_txt is passed needs to be vetted
        if "report_txt" in self.kwargs:
            if entity_type == "Upload":
                return Statuses.UPLOAD_INVALID
            elif entity_type == "Dataset":
                return Statuses.DATASET_INVALID
        else:
            if entity_type == "Upload":
                return Statuses.UPLOAD_ERROR
            elif entity_type == "Dataset":
                return Statuses.DATASET_ERROR
        raise Exception(f"Unknown entity_type: {entity_type}, cannot change status.")

    # TODO: on hold until specifics of notifications are figured out
    # def get_failure_email_template(
    #     self,
    #     formatted_exception=None,
    #     external_template=False,
    #     submission_data=None,
    #     report_txt=False,
    # ):
    #     if external_template:
    #         if report_txt and submission_data:
    #             subject = f"Your {submission_data.get('entity_type')} has failed!"
    #             msg = f"""
    #                 Error: {report_txt}
    #                 """
    #             return subject, msg
    #     else:
    #         if report_txt and submission_data:
    #             subject = f"{submission_data.get('entity_type')} {self.uuid} has failed!"
    #             msg = f"""
    #                 Error: {report_txt}
    #                 """
    #             return subject, msg
    #     return super().get_failure_email_template(formatted_exception)
    #
    # def send_failure_email(self, **kwargs):
    #     super().send_failure_email(**kwargs)
    #     if "report_txt" in kwargs:
    #         try:
    #             created_by_user_email = self.submission_data.get("created_by_user_email")
    #             subject, msg = self.get_failure_email_template(
    #                 formatted_exception=None,
    #                 external_template=True,
    #                 submission_data=self.submission_data,
    #                 **kwargs,
    #             )
    #             send_email(to=[created_by_user_email], subject=subject, html_content=msg)
    #         except:
    #             raise FailureCallbackException(
    #                 "Failure retrieving created_by_user_email or sending email in ValidateUploadFailure."
    #             )
