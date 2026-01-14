from typing import Optional
from pathlib import Path

import pandas as pd
import utils

from extra_utils import calculate_statistics

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
        status,
        uuid,
        token,
        messages=None,
        *args,
        **kwargs,
    ):
        super().__init__(status, uuid, token, messages, args, kwargs)
        self.dataset_type = self.entity_data.get("dataset_type", "")

    @property
    def is_derived(self):
        return True if self.entity_data.get("creation_action", "") == "Central Process" else False

    @property
    def is_valid_for_status(self):
        return True if self.status in self.good_statuses and self.is_derived else False

    def update(self):
        if self.status in self.good_statuses and self.is_derived:
            self.path = get_abs_path(self.uuid, self.token)
            print(f"UUID {self.uuid}")
            print(f"Dataset Type {self.dataset_type}")
            print(f"Path {self.path}")
            df = pd.DataFrame({"uuid": self.uuid,
                               "dataset_type": self.dataset_type,
                               "directory": self.path}, index=[0])
            statistics_path = log_directory_path(self.run_id) + "/datasets.csv"
            print(f"Statistics Path {statistics_path}")
            df.to_csv(Path(statistics_path), index=False)
            df = calculate_statistics(statistics_path)
            df.to_csv(Path(utils.get_statistics_base_path() / "dataset_usage.csv"), mode="a",
                      index=False, header=False)
