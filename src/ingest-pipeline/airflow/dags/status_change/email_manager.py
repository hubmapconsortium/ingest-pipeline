from .status_utils import (
    Statuses,
    get_entity_ingest_url,
    get_submission_context,
    is_internal_error,
)


class EmailManager:
    # TODO; name: email
    int_recipients = {}

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
        self.msg = str(msg) if msg else None
        self.run_id = run_id
        self.entity_data = get_submission_context(self.token, self.uuid)
        self.is_internal_error = is_internal_error(self.entity_data)
        # self.is_valid_for_status =
        self.entity_type = self.entity_data.get("entity_type")
        self.entity_id = self.entity_data.get("hubmap_id")

    def update(self):
        if self.is_internal_error:
            subj, msg = self.internal_error_format()
        # TODO: do we want to email for all of these?
        elif Statuses.get_status_str(self.status) in ["qa", "reorganized", "valid"]:
            subj, msg = self.generic_good_status_format()
        else:
            # Try to find correct messaging class
            # TODO
            subj, msg = "", ""
        self.send_email(subj, msg)

    def generic_good_status_format(self) -> tuple[str, str]:
        entity_status = self.entity_data.get("status")
        subj = (
            f"{self.entity_type} {self.entity_id} has successfully reached status {entity_status}!"
        )
        msg = f"View ingest record: {get_entity_ingest_url(self.run_id, self.entity_data)}"
        return subj, msg

    def internal_error_format(self) -> tuple[str, str]:
        from utils import get_tmp_dir_path

        subj = f"Internal error for {self.entity_type} {self.entity_id}"
        msg = f"""
        HuBMAP ID: {self.entity_id}
        UUID: {self.uuid}
        Entity type: {self.entity_type}
        Status: {Statuses.get_status_str(self.status)}
        Group: {self.entity_data.get('group_name')}
        Primary contact: {" | ".join([f'{name}: {email}' for name, email in self.primary_contact])}
        Ingest page: {get_entity_ingest_url(self.run_id, self.entity_data)}
        Log file: {get_tmp_dir_path(self.run_id)}
        Error: TODO - ???
        """
        return subj, msg

    def send_email(self, subj: str, msg: str):
        self.get_recipients()

    def get_recipients(self):
        if self.is_internal_error:
            self.main_recipient = self.int_recipients
        else:
            self.main_recipient = self.primary_contact
            self.cc = self.int_recipients

    @property
    def primary_contact(self) -> dict[str, str]:
        return {
            self.entity_data.get("created_by_user_displayname", ""): self.entity_data.get(
                "created_by_user_email", ""
            )
        }


"""
Organize by status; very likely to have multi-file layout like slack to allow for ~lots of formatting~
process:
    - figure out how to send emails
    - get correct subclass (method?)
"""
