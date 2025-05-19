# Run from dags dir as `python -m unittest status_change.tests.test_status_changer`
import unittest
from functools import cached_property
from unittest.mock import MagicMock, patch

from status_change.status_manager import (
    EntityUpdateException,
    EntityUpdater,
    StatusChanger,
    Statuses,
)
from utils import pythonop_set_dataset_state


class TestEntityUpdater(unittest.TestCase):
    validation_msg = "Test validation message"
    ingest_task = "Plugins passed: test_1, test_2"
    good_context = {
        "validation_message": "existing validation_message text",
        "ingest_task": "existing ingest_task text",
        "unrelated_field": True,
        "entity_type": "Upload",
        "status": "new",
    }

    @cached_property
    @patch("status_change.status_manager.get_submission_context")
    def upload_entity_valid(self, context_mock):
        context_mock.return_value = self.good_context
        return EntityUpdater(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"validation_message": self.validation_msg},
            fields_to_append_to={"ingest_task": self.ingest_task},
        )

    def test_get_entity_type(self):
        assert self.upload_entity_valid.entity_type == "Upload"

    def test_fields_to_change(self):
        assert self.upload_entity_valid.fields_to_change == {
            "ingest_task": f"existing ingest_task text | {self.ingest_task}",
            "validation_message": self.validation_msg,
        }

    @patch("status_change.status_manager.EntityUpdater._validate_fields_to_change")
    @patch("status_change.status_manager.HttpHook.run")
    def test_update(self, hhr_mock, validate_mock):
        hhr_mock.assert_not_called()
        validate_mock.assert_not_called()
        self.upload_entity_valid.update()
        validate_mock.assert_called_once()
        hhr_mock.assert_called_once()

    @patch("status_change.status_manager.StatusChanger")
    @patch("status_change.status_manager.get_submission_context")
    def test_pass_to_statuschanger(self, context_mock, statuschanger_mock):
        context_mock.return_value = self.good_context
        with_status = EntityUpdater(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"status": "valid", "validation_message": self.validation_msg},
            fields_to_append_to={"ingest_task": self.ingest_task},
        )
        with patch("status_change.status_manager.HttpHook.run"):
            with_status._validate_fields_to_change()
        assert statuschanger_mock.called_once_with(
            "upload_valid_uuid",
            "upload_valid_token",
            with_status.http_conn_id,
            {"validation_message": self.validation_msg},
            with_status.fields_to_append_to,
            with_status.delimiter,
            with_status.extra_options,
            with_status.verbose,
            "valid",
            with_status.entity_type,
        )

    @cached_property
    def upload_valid(self):
        with patch("status_change.status_manager.get_submission_context") as mock_mthd:
#            mock_mthd.return_value={"entity_type":"Upload"}
            return StatusChanger(
                "upload_valid_uuid",
                "upload_valid_token",
                status="Valid",
                extra_options={},
                entity_type="Upload",
            )

    def test_unrecognized_status(self):
        with patch("status_change.status_manager.get_submission_context"):
            with self.assertRaises(EntityUpdateException):
                StatusChanger(
                    "invalid_status_uuid",
                    "invalid_status_token",
                    status="invalid_status_string",
                    extra_options={},
                    entity_type="Upload",
                )

    def test_recognized_status(self):
        self.upload_valid._validate_fields_to_change()
        self.assertEqual(self.upload_valid.fields_to_change["status"], self.upload_valid.status)

    def test_extra_fields(self):
        with patch("status_change.status_manager.get_submission_context"):
            with_extra_field = StatusChanger(
                "extra_field_uuid",
                "extra_field_token",
                status=Statuses.UPLOAD_PROCESSING,
                fields_to_overwrite={"test_extra_field": True},
            )
            data = with_extra_field.fields_to_change
            self.assertIn("test_extra_field", data)
            self.assertEqual(data["test_extra_field"], True)

    @patch("status_change.status_manager.HttpHook.run")
    def test_extra_options(self, hhr_mock):
        with patch("status_change.status_manager.get_submission_context"):
            with_extra_option = StatusChanger(
                "extra_options_uuid",
                "extra_options_token",
                status=Statuses.UPLOAD_VALID,
                extra_options={"check_response": False},
                verbose=False,
            )
            with_extra_option._set_entity_api_data()
            self.assertIn({"check_response": False}, hhr_mock.call_args.args)
            without_extra_option = StatusChanger(
                "extra_options_uuid",
                "extra_options_token",
                status=Statuses.UPLOAD_VALID,
                verbose=False,
            )
            without_extra_option._set_entity_api_data()
            self.assertIn({"check_response": True}, hhr_mock.call_args.args)

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.HttpHook.run")
    def test_extra_options_and_fields_good(self, hhr_mock, context_mock):
        context_mock.return_value = {
            "status": "processing",
            "entity_type": "Upload",
            "test_extra_field": False,
        }
        with_extra_option_and_field = StatusChanger(
            "extra_options_uuid",
            "extra_options_token",
            status=Statuses.UPLOAD_VALID,
            fields_to_overwrite={"test_extra_field": True},
            extra_options={"check_response": False},
            verbose=False,
        )
        with_extra_option_and_field.update()
        self.assertIn({"check_response": False}, hhr_mock.call_args.args)
        self.assertIn('{"test_extra_field": true, "status": "valid"}', hhr_mock.call_args.args)

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.HttpHook.run")
    def test_extra_fields_bad(self, hhr_mock, context_mock):
        context_mock.return_value = {
            "status": "processing",
            "entity_type": "Upload",
        }
        with_extra_option_and_field = StatusChanger(
            "extra_options_uuid",
            "extra_options_token",
            status=Statuses.UPLOAD_VALID,
            fields_to_overwrite={"test_extra_field": True},
            verbose=False,
        )
        self.assertRaises(Exception, with_extra_option_and_field.update)
        hhr_mock.assert_not_called()

    @patch("status_change.status_manager.HttpHook.run")
    def test_valid_status_in_request(self, hhr_mock):
        self.upload_valid._validate_fields_to_change()
        self.upload_valid._set_entity_api_data()
        self.assertIn('{"status": "valid"}', hhr_mock.call_args.args)

    def test_http_conn_id(self):
        with patch("status_change.status_manager.HttpHook.run"):
            with_http_conn_id = StatusChanger(
                "http_conn_uuid",
                "http_conn_token",
                status=Statuses.DATASET_NEW,
                http_conn_id="test_conn_id",
            )
            assert with_http_conn_id.http_conn_id == "test_conn_id"

    @patch("status_change.status_manager.StatusChanger.send_email")
    @patch("status_change.status_manager.StatusChanger.status_map")
    def test_update_from_status_map(self, status_map_mock, send_email_mock):
        status_map_mock.get.side_effect = {Statuses.UPLOAD_VALID: [("send_email", {})]}.get
        self.assertFalse(send_email_mock.called)
        self.assertEqual(self.upload_valid.status, Statuses.UPLOAD_VALID)
        with patch("status_change.status_manager.HttpHook.run"):
            self.upload_valid.update()
        self.assertTrue(send_email_mock.called)

    @staticmethod
    def my_callable(**kwargs):
        return kwargs["uuid"]

    @patch("utils.StatusChanger")
    @patch("utils.get_auth_tok")
    def test_pythonop_set_dataset_state_valid(self, gat_mock, sc_mock):
        uuid = "test_uuid"
        token = "test_token"
        gat_mock.return_value = token
        message = "Test message"
        # Not passing a ds_state kwarg sets status to Processing
        dag_run_mock = MagicMock(conf={"dryrun": False})
        pythonop_set_dataset_state(
            crypt_auth_tok=token,
            dataset_uuid_callable=self.my_callable,
            uuid=uuid,
            message=message,
            dag_run=dag_run_mock,
        )
        sc_mock.assert_called_with(
            uuid,
            token,
            status="Processing",
            fields_to_overwrite={"pipeline_message": message},
            http_conn_id="entity_api_connection",
        )
        # Pass a valid ds_state and assert it was passed properly
        pythonop_set_dataset_state(
            crypt_auth_tok=token,
            dataset_uuid_callable=self.my_callable,
            uuid=uuid,
            message=message,
            ds_state="QA",
            dag_run=dag_run_mock,
        )
        sc_mock.assert_called_with(
            uuid,
            token,
            status="QA",
            fields_to_overwrite={"pipeline_message": message},
            http_conn_id="entity_api_connection",
        )

    @patch("utils.get_auth_tok")
    def test_pythonop_set_dataset_state_invalid(self, gat_mock):
        with patch("status_change.status_manager.HttpHook.run"):
            uuid = "test_uuid"
            token = "test_token"
            gat_mock.return_value = token
            message = "Test message"
            # Pass an invalid ds_state
            with self.assertRaises(Exception):
                pythonop_set_dataset_state(
                    crypt_auth_tok=token,
                    dataset_uuid_callable=self.my_callable,
                    uuid=uuid,
                    message=message,
                    ds_state="Unknown",
                )

    context_mock_value = {
        "uuid": "test_uuid",
        "hubmap_id": "test_hm_id",
        "created_by_user_displayname": "Test User",
        "created_by_user_email": "test@user.com",
        "priority_project_list": ["test_project, test_project_2"],
        "datasets": [
            {
                "uuid": "test_dataset_uuid",
                "hubmap_id": "test_dataset_hm_id",
                "created_by_user_displayname": "Test User",
                "created_by_user_email": "test@user.com",
                "priority_project_list": ["test_project", "test_project_2"],
                "dataset_type": "test_dataset_type",
                "metadata": [{"parent_dataset_id": "test_parent_id"}],
            }
        ],
        "entity_type": "Upload",
        "status": "New",
    }

    @patch("status_change.status_manager.StatusChanger.send_slack_message")
    @patch("status_change.slack_formatter.get_organ")
    @patch("status_change.slack_formatter.get_submission_context")
    def test_upload_reorganized_priority_slack(self, context_mock, organ_mock, slack_mock):
        with patch("status_change.status_manager.get_submission_context"):
            with patch("status_change.status_manager.StatusChanger._set_entity_api_data"):
                context_mock.return_value = self.context_mock_value
                organ_mock.return_value = "TO"
                StatusChanger(
                    "upload_valid_uuid",
                    "upload_valid_token",
                    status="Reorganized",
                    extra_options={},
                    entity_type="Upload",
                ).update()
                slack_mock.assert_called_once()
                organ_mock.assert_called_with("test_dataset_uuid", "upload_valid_token")

    @patch("status_change.status_manager.StatusChanger.send_slack_message")
    @patch("status_change.status_manager.get_submission_context")
    def test_upload_reorganized_not_priority(self, context_mock, slack_mock):
        with patch("status_change.status_manager.StatusChanger._set_entity_api_data"):
            new_context = self.context_mock_value.copy()
            new_context.pop("priority_project_list")
            context_mock.return_value = new_context
            StatusChanger(
                "upload_valid_uuid",
                "upload_valid_token",
                status="Reorganized",
                extra_options={},
                entity_type="Upload",
            ).update()
            slack_mock.assert_not_called()

    @patch("status_change.status_manager.StatusChanger.send_slack_message")
    @patch("status_change.status_manager.get_submission_context")
    def test_slack_not_triggered(self, context_mock, slack_mock):
        with patch("status_change.status_manager.StatusChanger._set_entity_api_data"):
            context_mock.return_value = self.context_mock_value
            StatusChanger(
                "upload_valid_uuid",
                "upload_valid_token",
                status="Submitted",
                extra_options={},
                entity_type="Upload",
            ).update()
            slack_mock.assert_not_called()

    @patch("status_change.slack_formatter.airflow_conf.as_dict")
    @patch("status_change.slack_formatter.get_organ")
    @patch("status_change.slack_formatter.get_submission_context")
    def test_check_priority_reorganized(self, context_mock, organ_mock, airflow_conf_mock):
        organ_mock.return_value = "TO"
        context_mock.return_value = self.context_mock_value
        channel_val = "test_channel"
        airflow_conf_mock.return_value = {
            "slack_channels": {"PRIORITY_UPLOAD_REORGANIZED": channel_val}
        }
        msg, channel = format_priority_reorganized_msg("test_token", "test_uuid")
        assert channel == channel_val
        assert (
            msg
            == "Priority upload (test_project, test_project_2) reorganized:\n   uuid: test_uuid\n   hubmap_id: test_hm_id\n   created_by_user_displayname: Test User\n   created_by_user_email: test@user.com\n   priority_project_list: test_project, test_project_2\n   dataset_type: test_dataset_type\n   organ: TO\n\nDatasets:\nhubmap_id, created_by_user_displayname, created_by_user_email, priority_project_list, dataset_type, organ\ntest_dataset_hm_id, Test User, test@user.com, test_project;test_project_2, test_dataset_type, TO"
        )


# if __name__ == "__main__":
#     suite = unittest.TestLoader().loadTestsFromTestCase(TestEntityUpdater)
#     suite.debug()
