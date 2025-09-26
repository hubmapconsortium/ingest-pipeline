from utils import get_tmp_dir_path

from .status_utils import (
    Statuses,
    get_entity_ingest_url,
    get_project,
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
        self.status = Statuses.get_status_str(status)
        self.msg = str(msg) if msg else None
        self.run_id = run_id
        self.entity_data = get_submission_context(self.token, self.uuid)
        self.project = get_project()
        self.is_internal_error = is_internal_error(self.entity_data)
        self.is_valid_for_status = bool(self.get_message_content)
        self.entity_type = self.entity_data.get("entity_type")
        # Get hubmap_id or sennet_id
        self.entity_id = self.entity_data.get(f"{get_project().value[0]}_id")

    def update(self):
        self.send_email(*self.get_message_content)

    @property
    def get_message_content(self) -> tuple[str, str]:
        if self.is_internal_error:  # error, potentially invalid
            subj, msg = self.internal_error_format()
        # TODO: do we want to email for all of these?
        elif self.status in ["qa", "reorganized", "valid"]:
            subj, msg = self.generic_good_status_format()
        else:  # actually invalid
            subj, msg = self.get_ext_invalid_format()
        return subj, msg

    def send_email(self, subj: str, msg: str):
        # TODO
        self.get_recipients()

    def generic_good_status_format(self) -> tuple[str, str]:
        subj = (
            f"{self.entity_type} {self.entity_id} has successfully reached status {self.status}!"
        )
        msg = f"View ingest record: {get_entity_ingest_url(self.entity_data)}"
        return subj, msg

    def internal_error_format(self) -> tuple[str, str]:
        from utils import get_tmp_dir_path

        subj = f"Internal error for {self.entity_type} {self.entity_id}"
        msg = f"""
        {self.project.value[1]} ID: {self.entity_id}
        UUID: {self.uuid}
        Entity type: {self.entity_type}
        Status: {self.status}
        Group: {self.entity_data.get('group_name')}
        Primary contact: {" | ".join([f'{name}: {email}' for name, email in self.primary_contact])}
        Ingest page: {get_entity_ingest_url(self.entity_data)}
        Log file: {get_tmp_dir_path(self.run_id)}

        Error: {self.entity_data.get('error_message')}
        """
        return subj, msg

    def get_ext_invalid_format(self) -> tuple[str, str]:
        subj = (
            f"{self.entity_type} {self.entity_id} has successfully reached status {self.status}!"
        )
        msg = f"""
        {self.project.value[1]} ID: {self.entity_id}
        Status: {self.status}
        Group: {self.entity_data.get('group_name')}
        Primary contact: {" | ".join([f'{name}: {email}' for name, email in self.primary_contact])}
        Ingest page: {get_entity_ingest_url(self.entity_data)}
        Log file: {get_tmp_dir_path(self.run_id)}

        Error: {self.entity_data.get('error_message')}
        """
        return subj, msg

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
