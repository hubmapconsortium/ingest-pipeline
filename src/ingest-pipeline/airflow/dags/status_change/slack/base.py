from status_change.status_utils import (
    get_abs_path,
    get_data_ingest_board_query_url,
    get_entity_ingest_url,
    get_globus_url,
    get_primary_dataset,
    get_project,
    get_submission_context,
    slack_channels,
)


class SlackMessage:
    # Name should match what's in status_utils.slack_channels
    name = "base"

    def __init__(
        self,
        uuid: str,
        token: str,
        run_id: str | None = None,
        processing_pipeline: str | None = None,
    ):
        self.uuid = uuid
        self.token = token
        self.run_id = run_id
        self.processing_pipeline = processing_pipeline
        self.channel = slack_channels.get(self.name, "")
        self.entity_id_str = f"{get_project().value[0]}_id"  # "hubmap_id" or "sennet_id"
        self.entity_data = get_submission_context(token, uuid)
        self.entity_id = self.entity_data.get(self.entity_id_str)

    @classmethod
    def test(cls, entity_data: dict, **kwargs) -> bool:
        """
        If there are special case subclasses for a given status, their
        test() methods will be called to determine if the subclass applies.
        Only one should return True because the subclass test loop breaks
        after first True result.

        entity_data - metadata from Entity API
        possible kwargs:
            processing_pipeline: str - name of triggering pipeline;
                                 only used for processing pipelines
        """
        del entity_data, kwargs
        return False

    def format(self) -> list:
        raise NotImplementedError

    @property
    def ingest_ui_url(self) -> str:
        return get_entity_ingest_url(self.entity_data)

    @property
    def data_ingest_board_url(self) -> str:
        if self.primary_dataset_info:
            return get_data_ingest_board_query_url(self.primary_dataset_info)
        return get_data_ingest_board_query_url(self.entity_data)

    @property
    def entity_links(self) -> list[str]:
        """
        View on Ingest UI.
        View [primary dataset] on Data Ingest Board.
        View on Globus.
        Filesystem path: /path/to/data
        """
        msg = [f"<{self.ingest_ui_url}|View on Ingest UI.>"]
        if self.primary_dataset_info:
            msg.append(
                f"<{self.data_ingest_board_url}|View primary dataset on Data Ingest Board.>"
            )
        else:
            msg.append(f"<{self.data_ingest_board_url}|View on Data Ingest Board.>")
        msg.extend(
            [
                f"<{get_globus_url(self.entity_data, self.token)}|View on Globus.>",
                f"Filesystem path: {self.copyable_filepath}",
            ]
        )
        return msg

    @property
    def copyable_filepath(self):
        return get_abs_path(self.uuid, self.token, escaped=True)

    @property
    def primary_dataset_info(self) -> dict | None:
        primary_dataset_uuid = get_primary_dataset(self.entity_data, self.token)
        if primary_dataset_uuid:
            return get_submission_context(self.token, primary_dataset_uuid)

    def create_primary_link(self) -> str | None:
        if self.primary_dataset_info:
            return f"<{get_entity_ingest_url(self.primary_dataset_info)}|{self.primary_dataset_info.get(self.entity_id_str)}>"
