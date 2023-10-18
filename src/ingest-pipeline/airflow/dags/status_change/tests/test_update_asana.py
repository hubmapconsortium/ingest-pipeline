import unittest
from copy import deepcopy
from unittest.mock import patch

import asana
from status_manager import StatusChangerException, Statuses, UpdateAsana
from tests.fixtures import (
    custom_fields_fixture,
    custom_submission_data_fixture,
    search_tasks_fixture_invalid,
    search_tasks_fixture_valid,
    task_updated_fixture,
    task_updated_reorg_fixture,
)


# TODO: BROKEN
@patch("status_manager.HttpHook.run")
@patch("status_manager.asana.Client")
@patch.object(UpdateAsana, "hubmap_id", {"hubmap_id.return_value": "test_hubmap_id"})
@patch.object(
    UpdateAsana,
    "submission_data",
    return_value=custom_submission_data_fixture,
)
class TestUpdateAsana(unittest.TestCase):
    @patch.object(UpdateAsana, "get_custom_fields", return_value=custom_fields_fixture)
    def setUp(self, *args):
        self.ua = UpdateAsana(
            "test_uuid",
            "test_token",
            Statuses.UPLOAD_VALID,
            workspace_id="test_workspace",
            project_id="test_project",
        )

    def test_custom_field_gid(self, *args):
        self.assertIsNotNone(self.ua.process_stage_gids)
        self.assertEqual(
            list(self.ua.asana_status_map.values()).sort(),
            list(self.ua.process_stage_gids.values()).sort(),
        )
        # Why is this working with submission_data being a cached property?
        self.assertIsNotNone(self.ua.submission_data())
        self.assertIn("datasets", self.ua.submission_data())

    @patch.object(UpdateAsana, "get_custom_fields", return_value={})
    def test_missing_custom_field_gid(self, *args):
        with self.assertRaises(Exception):
            UpdateAsana(
                "test_invalid_uuid",
                "test_invalid_token",
                Statuses.UPLOAD_VALID,
                workspace_id="test_invalid_workspace",
                project_id="test_invalid_project",
            )

    @patch.object(
        asana.client.resources.tasks.Tasks,
        "search_tasks_for_workspace",
        return_value=[{"gid": "test_task_gid"}],
    )
    def test_get_task_returns_one(self, *args):
        self.assertEqual(self.ua.get_task_by_hubmap_id, "test_task_gid")

    @patch.object(
        asana.client.resources.tasks.Tasks,
        "search_tasks_for_workspace",
        return_value=[],
    )
    def test_get_task_returns_none(self, *args):
        with self.assertRaises(StatusChangerException):
            self.ua.get_task_by_hubmap_id

    @patch.object(
        asana.client.resources.tasks.Tasks,
        "search_tasks_for_workspace",
        return_value=search_tasks_fixture_valid,
    )
    def test_get_task_returns_two_valid(self, *args):
        self.assertEqual(self.ua.get_task_by_hubmap_id, search_tasks_fixture_valid[0]["gid"])

    @staticmethod
    def modify_search_tasks_fixture_data():
        valid_modified = deepcopy(search_tasks_fixture_valid)
        print(valid_modified[0])
        valid_modified[0]["custom_fields"][0]["enum_value"]["name"] = "Test"
        return valid_modified

    @patch.object(
        asana.client.resources.tasks.Tasks,
        "search_tasks_for_workspace",
        return_value=modify_search_tasks_fixture_data(),
    )
    def test_get_task_returns_two_invalid(self, *args):
        with self.assertRaises(StatusChangerException):
            self.ua.get_task_by_hubmap_id

    @patch.object(
        asana.client.resources.tasks.Tasks,
        "search_tasks_for_workspace",
        return_value=search_tasks_fixture_invalid,
    )
    def test_get_task_returns_three(self, *args):
        with self.assertRaises(StatusChangerException):
            self.ua.get_task_by_hubmap_id

    def test_get_asana_status_valid(self, *args):
        self.assertEqual(self.ua.get_asana_status, self.ua.asana_status_map[Statuses.UPLOAD_VALID])

    def test_get_asana_status_invalid(self, *args):
        self.ua.status = None
        with self.assertRaises(StatusChangerException):
            self.ua.get_asana_status
        self.ua.status = Statuses.UPLOAD_VALID

    @patch.object(
        asana.client.resources.tasks.Tasks, "update_task", return_value=task_updated_fixture
    )
    @patch.object(UpdateAsana, "get_task_by_hubmap_id", "test_hubmap_task")
    def test_update_process_stage_data(self, *args):
        self.ua.status = Statuses.UPLOAD_VALID
        self.assertEqual(
            self.ua.get_custom_field_gid("Process Stage"),
            custom_fields_fixture[1]["custom_field"]["gid"],
        )
        self.assertEqual(self.ua.get_asana_status, self.ua.asana_status_map[self.ua.status])
        self.ua.update_process_stage()

    @patch.object(
        asana.client.resources.tasks.Tasks,
        "search_tasks_for_workspace",
        return_value=search_tasks_fixture_valid,
    )
    @patch.object(
        asana.client.resources.tasks.Tasks, "update_task", return_value=task_updated_reorg_fixture
    )
    @patch.object(UpdateAsana, "create_dataset_cards")
    def test_update_process_stage_with_reorganize(self, *args):
        self.ua.status = Statuses.UPLOAD_REORGANIZED
        self.ua.update_process_stage()
        self.assertTrue(self.ua.create_dataset_cards.called)

    def test_create_dataset_cards_timestamp_bad(self, *args):
        bad_submission_data = deepcopy(custom_submission_data_fixture)
        bad_submission_data["datasets"][0]["created_timestamp"] = None
        with self.assertRaises(Exception):
            with patch.object(UpdateAsana, "submission_data", return_value=bad_submission_data):
                self.ua.create_dataset_cards()

    @patch.object(
        UpdateAsana,
        "submission_data",
        return_value=custom_submission_data_fixture,
    )
    @patch.object(
        asana.client.resources.tasks.Tasks,
        "create_task",
    )
    def test_create_dataset_cards_data(self, create_task_mock, *args):
        self.ua.create_dataset_cards()
        create_task_mock.assert_called_once
        create_task_mock.assert_called_with(
            {
                "name": f"{custom_submission_data_fixture['datasets'][0]['group_name']} | {custom_submission_data_fixture['datasets'][0]['ingest_metadata']['metadata']['assay_type']} | {self.ua.convert_utc_timestamp(custom_submission_data_fixture['datasets'][0])}",
                "custom_fields": {
                    custom_fields_fixture[0]["custom_field"][
                        "gid"
                    ]: custom_submission_data_fixture["datasets"][0]["hubmap_id"],
                    custom_fields_fixture[1]["custom_field"]["gid"]: self.ua.process_stage_gids[
                        "Intake"
                    ],
                },
                "projects": ["test_project"],
            },
            opt_pretty=True,
        )
