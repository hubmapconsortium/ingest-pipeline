import unittest
from unittest.mock import patch

import pandas as pd

# Create appropriate fake airflow.cfg
with patch(
    "airflow.configuration.conf.as_dict",
    return_value={"connections": {"APP_CLIENT_SECRET": "test"}},
):
    with patch("utils.encrypt_tok"):
        from email_providers import (
            add_instructions,
            format_group_data,
            get_counts,
            get_email_body_list,
            get_template,
            modify_df,
        )

from status_change.status_utils import Statuses
from tests.fixtures import (
    df,
    dp_email_body_list,
    dp_instructions,
    footer,
    formatted_group_data,
    header,
)


class TestEmailProvidersDAG(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame.from_dict(df, orient="index")  # type: ignore
        self.modified_df = modify_df(self.df)
        self.group_name = "Beth Israel Deaconess Medical Center TMC"

    def test_modify_df(self):
        self.df["_source.last_modified_timestamp"]
        modified = modify_df(self.df)
        with self.assertRaises(KeyError):
            modified["_source.last_modified_timestamp"]
        modified["last_modified_date"]

    def test_get_counts(self):
        counts = get_counts(self.modified_df)
        assert counts == [
            "<li>Error: 1</li>",
            "<li>Invalid: 1</li>",
            "<li>New: 1</li>",
            "<li>QA: 8</li>",
        ]
        qa_counts = get_counts(self.modified_df, [Statuses.DATASET_QA])
        assert qa_counts == ["<li>QA: 8</li>"]
        no_counts = get_counts(self.modified_df, [Statuses.UPLOAD_VALID])
        assert no_counts == []

    def test_add_instructions(self):
        instructions = add_instructions(self.modified_df)
        assert instructions == dp_instructions

    def test_get_email_body_list(self):
        email_body_list = get_email_body_list(self.modified_df)
        assert email_body_list == dp_email_body_list

    def test_get_template(self):
        instructions = add_instructions(self.modified_df)
        counts = get_counts(self.modified_df)
        assert [
            *header(len(self.modified_df), self.group_name),
            *counts,
            *instructions,
            *footer,
        ] == get_template(self.modified_df, self.group_name)

    def test_format_group_data(self):
        assert format_group_data(self.modified_df, self.group_name) == formatted_group_data
