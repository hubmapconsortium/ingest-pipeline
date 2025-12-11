from typing import Optional
from pathlib import Path

import pandas as pd

from extra_utils import calculate_statistics
from utils import get_statistics_base_path

from status_change.status_utils import (
    MessageManager,
    Statuses,
    get_abs_path,
    log_directory_path,
)


class StatisticsManager(MessageManager):
    good_statuses = [
        Statuses.DATASET_QA,
    ]

    def __init__(
        self,
        status: Statuses,
        uuid: str,
        token: str,
        messages: Optional[dict] = None,
        run_id: str = "",
        *args,
        **kwargs,
    ):
        super().__init__(status, uuid, token, messages, run_id, args, kwargs)
        self.dataset_type = self.entity_data.get("dataset_type", "")

    @property
    def is_valid_for_status(self):
        return bool(self.subj and self.msg)

    def update(self):
        if self.status in self.good_statuses:
            self.path = get_abs_path(self.uuid, self.token)
            print("UUID {self.uuid}")
            print("Dataset Type {self.dataset_type}")
            print("Path {self.path}")
            df = pd.DataFrame({"uuid": self.uuid,
                               "dataset_type": self.dataset_type,
                               "path": self.path})
            statistics_path = log_directory_path(self.run_id) / "datasets.csv"
            df.to_csv(Path(statistics_path), index=False)
            df = calculate_statistics(statistics_path)
            df.to_csv(Path(get_statistics_base_path() / "dataset_usage.csv"), mode="a", index=False)
