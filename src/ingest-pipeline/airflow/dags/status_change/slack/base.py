from typing import Optional
from urllib.parse import urlencode, urljoin

from status_change.status_utils import (
    get_abs_path,
    get_hubmap_id_from_uuid,
    get_submission_context,
)

from airflow.configuration import conf as airflow_conf


class SlackMessage:
    # Name should match what's in airflow_conf.slack_channels
    name = "base"

    def __init__(self, uuid: str, token: str, entity_data: Optional[dict] = None):
        self.uuid = uuid
        self.token = token
        self.channel = airflow_conf.as_dict().get("slack_channels", {}).get(self.name.upper())
        self.entity_data = entity_data if entity_data else get_submission_context(token, uuid)
        # TODO: does this need to be SenNet aware?
        self.data_ingest_board = "https://ingest.board.hubmapconsortium.org/"

    @classmethod
    def test(cls, entity_data: dict) -> bool:
        """
        If there are special case subclasses for a given status, their
        test() methods will be called to determine if the subclass applies.
        Only one should return True because the subclass test loop breaks
        after first True result.
        """
        return False

    def format(self) -> str:
        raise NotImplementedError

    @property
    def data_ingest_board_query_url(self):
        return urljoin(
            self.data_ingest_board, f"?q={get_hubmap_id_from_uuid(self.token, self.uuid)}"
        )

    @property
    def dataset_links(self):
        """
        View on Data Ingest Board.
        View on Globus.
        Filesystem path: /path/to/data
        """
        return f"""<{self.data_ingest_board_query_url}|View on Data Ingest Board.>
        <{self.get_globus_url()}|View on Globus.>
        Filesystem path: {get_abs_path(self.uuid, self.token)}"""

    def get_globus_url(self, uuid: Optional[str] = None) -> str:
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
        params = {}
        if "public" in path:
            params["origin_id"] = "af603d86-eab9-4eec-bb1d-9d26556741bb"
            params["origin_path"] = lookup_uuid
        else:
            params["origin_id"] = "24c2ee95-146d-4513-a1b3-ac0bfdb7856f"
            params["origin_path"] = path.replace("/hive/hubmap/data", "") + "/"
        return prefix + urlencode(params)
