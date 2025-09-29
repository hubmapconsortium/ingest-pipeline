from typing import Optional
from urllib.parse import urlencode

from status_change.status_utils import (
    get_abs_path,
    get_data_ingest_board_query_url,
    get_entity_ingest_url,
    get_project,
    get_submission_context,
    globus_dirs,
    slack_channels,
)


class SlackMessage:
    # Name should match what's in status_utils.slack_channels
    name = "base"

    def __init__(self, uuid: str, token: str, entity_data: Optional[dict] = None):
        self.uuid = uuid
        self.token = token
        self.channel = slack_channels.get(self.name, "")
        self.entity_id_str = f"{get_project().value[0]}_id"
        self.entity_data = entity_data if entity_data else get_submission_context(token, uuid)

    @classmethod
    def get_channel(cls):
        return slack_channels.get(cls.name, "")

    @classmethod
    def test(cls, entity_data: dict, token: str) -> bool:
        """
        If there are special case subclasses for a given status, their
        test() methods will be called to determine if the subclass applies.
        Only one should return True because the subclass test loop breaks
        after first True result.
        """
        del entity_data, token
        return False

    def format(self) -> str:
        raise NotImplementedError

    @property
    def ingest_ui_url(self):
        return get_entity_ingest_url(self.entity_data)

    @property
    def data_ingest_board_url(self):
        return get_data_ingest_board_query_url(self.entity_data)

    @property
    def entity_links_str(self):
        """
        View on Ingest UI.
        View on Data Ingest Board.
        View on Globus.
        Filesystem path: /path/to/data
        """
        return f"""
        <{self.ingest_ui_url}|View on Ingest UI.>
        <{self.data_ingest_board_url}|View on Data Ingest Board.>
        <{self.get_globus_url()}|View on Globus.>
        Filesystem path: {self.copyable_filepath}
        """

    @property
    def copyable_filepath(self):
        return get_abs_path(self.uuid, self.token, escaped=True)

    def get_globus_url(self, uuid: Optional[str] = None) -> Optional[str]:
        """
        Return the Globus URL (default) for a dataset.
        URL format is https://app.globus.org/file-manager?origin_id=<id>&origin_path=<uuid | consortium|private/<group>/<uuid>>
        """
        if uuid:
            lookup_uuid = uuid
        else:
            lookup_uuid = self.uuid
        path = get_abs_path(lookup_uuid, self.token)
        prefix = "https://app.globus.org/file-manager?"
        proj = get_project()
        project_dict = globus_dirs.get(proj.value[0])
        if not project_dict:
            return
        params = {}
        if "public" in path:
            params["origin_id"] = project_dict.get("public")
            params["origin_path"] = lookup_uuid
        else:
            params["origin_id"] = project_dict.get("protected")
            params["origin_path"] = (
                path.replace(project_dict.get("path_replace_str", ""), "") + "/"
            )
        return prefix + urlencode(params)
