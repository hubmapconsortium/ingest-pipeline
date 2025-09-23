# Run from dags dir as `python -m unittest status_change.tests.test_status_changer`
import unittest
import logging
from functools import cached_property
from unittest.mock import MagicMock, patch
from datetime import date

from status_change.status_manager import (
    EntityUpdateException,
    EntityUpdater,
    StatusChanger,
    Statuses,
    StatusChangeAction,
)
from status_change.slack_formatter import format_priority_reorganized_msg
from status_change.failure_callback import FailureCallback
from utils import pythonop_set_dataset_state


def send_email():  # for patching later
    pass


class SendEmailSCA(StatusChangeAction):
    def run(self, context):
        send_email()


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
    def test_should_be_statuschanger(self, context_mock, statuschanger_mock):
        context_mock.return_value = self.good_context
        with_status = EntityUpdater(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"status": "valid", "validation_message": self.validation_msg},
            fields_to_append_to={"ingest_task": self.ingest_task},
        )
        with patch("status_change.status_manager.HttpHook.run") as hhr_mock:
            hhr_mock.return_value.json.return_value = self.good_context
            with self.assertRaises(EntityUpdateException):
                with_status._validate_fields_to_change()

    @cached_property
    def upload_valid(self):
        with patch("status_change.status_manager.get_submission_context") as mock_mthd:
            mock_mthd.return_value = self.good_context
            return StatusChanger(
                "upload_valid_uuid",
                "upload_valid_token",
                status="Valid",
                extra_options={},
                entity_type="Upload",
            )

    def test_unrecognized_status(self):
        with patch("status_change.status_manager.get_submission_context") as gsc_mock:
            gsc_mock.return_value = self.good_context
            with self.assertRaises(EntityUpdateException):
                StatusChanger(
                    "invalid_status_uuid",
                    "invalid_status_token",
                    status="Published",
                    extra_options={},
                    entity_type="Upload",
                )

    def test_recognized_status(self):
        self.upload_valid._validate_fields_to_change()
        self.assertEqual(self.upload_valid.fields_to_change["status"], self.upload_valid.status)

    def test_extra_fields(self):
        with patch("status_change.status_manager.get_submission_context") as gsc_mock:
            gsc_mock.return_value = self.good_context
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
        hhr_mock.return_value.json.return_value = self.good_context
        with patch("status_change.status_manager.get_submission_context") as gsc_mock:
            gsc_mock.return_value = self.good_context
            with_extra_option = StatusChanger(
                "extra_options_uuid",
                "extra_options_token",
                status=Statuses.UPLOAD_VALID.value,
                extra_options={"check_response": False},
                verbose=False,
            )
            with_extra_option._set_entity_api_data()
            self.assertIn({"check_response": False}, hhr_mock.call_args.args)
            without_extra_option = StatusChanger(
                "extra_options_uuid",
                "extra_options_token",
                status=Statuses.UPLOAD_VALID.value,
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
            status=Statuses.UPLOAD_VALID.value,
            fields_to_overwrite={"test_extra_field": True},
            extra_options={"check_response": False},
            verbose=False,
        )
        with_extra_option_and_field.update()
        self.assertIn({"check_response": False}, hhr_mock.call_args.args)
        self.assertIn('{"test_extra_field": true, "status": "valid"}', hhr_mock.call_args.args)

    @patch("status_change.status_manager.HttpHook.run")
    def test_valid_status_in_request(self, hhr_mock):
        self.upload_valid._validate_fields_to_change()
        self.upload_valid._set_entity_api_data()
        self.assertIn('{"status": "valid"}', hhr_mock.call_args.args)

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.HttpHook.run")
    def test_invalid_status_in_request(self, hhr_mock, ctx_mock):
        ctx_mock.return_value = {
            "status": "processing",
            "entity_type": "Upload",
            "test_extra_field": True,
        }
        sc = StatusChanger(
            "my_test_uuid",
            "my_test_token",
            status="Valid",
            extra_options={},
            entity_type="Upload",
            fields_to_overwrite={"test_extra_field": False},
            # fields_to_overwrite={
            #     #"status": "Published",
            #     "unrelated_field": False,
            # }
        )
        sc._validate_fields_to_change()
        sc.update()
        # patch the StatusChanger to avoid the tests in its constructor which
        # would normally detect an invalid update
        sc.fields_to_change["status"] = "published"
        with self.assertRaises((AssertionError, EntityUpdateException)):
            sc._set_entity_api_data()

    def test_http_conn_id(self):
        with patch("status_change.status_manager.HttpHook.run") as httpr_mock:
            httpr_mock.return_value.json.return_value = self.good_context
            with_http_conn_id = StatusChanger(
                "http_conn_uuid",
                "http_conn_token",
                status=Statuses.DATASET_NEW,
                http_conn_id="test_conn_id",
            )
            assert with_http_conn_id.http_conn_id == "test_conn_id"

    @patch("test_status_changer.send_email")
    @patch("status_change.status_manager.StatusChanger.status_map")
    def test_update_from_status_map(self, status_map_mock, send_email_mock):
        status_map_mock.get.side_effect = {Statuses.UPLOAD_VALID.value: [(SendEmailSCA, {})]}.get
        self.assertFalse(send_email_mock.called)
        self.assertEqual(self.upload_valid.status, Statuses.UPLOAD_VALID.value)
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
            retry=True,
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

    @patch("status_change.slack_formatter.get_organ")
    @patch("status_change.slack_formatter.get_submission_context")
    @patch("status_change.status_manager.HttpHook.run")
    @patch("status_change.slack_formatter.airflow_conf.as_dict")
    def test_upload_reorganized_priority_slack(
        self, af_conf_mock, hhr_mock, context_mock, organ_mock
    ):
        af_conf_mock.return_value = {
            "slack_channels": {"PRIORITY_UPLOAD_REORGANIZED": "test_channel"}
        }
        with patch("status_change.status_manager.get_submission_context") as mock_gsc:
            mock_gsc.return_value = self.context_mock_value
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
                hhr_mock.assert_called_once()
                organ_mock.assert_called_with("test_dataset_uuid", "upload_valid_token")

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.HttpHook.run")
    @patch("status_change.slack_formatter.airflow_conf.as_dict")
    def test_upload_reorganized_not_priority(self, af_conf_mock, hhr_mock, context_mock):
        af_conf_mock.return_value = {
            "slack_channels": {"PRIORITY_UPLOAD_REORGANIZED": "test_channel"}
        }
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
            hhr_mock.assert_not_called()

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.HttpHook.run")
    @patch("status_change.slack_formatter.airflow_conf.as_dict")
    def test_slack_not_triggered(self, af_conf_mock, hhr_mock, context_mock):
        af_conf_mock.return_value = {
            "slack_channels": {"PRIORITY_UPLOAD_REORGANIZED": "test_channel"}
        }
        with patch("status_change.status_manager.StatusChanger._set_entity_api_data"):
            context_mock.return_value = self.context_mock_value
            StatusChanger(
                "upload_valid_uuid",
                "upload_valid_token",
                status="Submitted",
                extra_options={},
                entity_type="Upload",
            ).update()
            hhr_mock.assert_not_called()

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

    @patch("status_change.status_manager.get_submission_context")
    @patch("traceback.TracebackException")
    @patch("status_change.failure_callback.get_auth_tok")
    def test_failure_callback(self, gat_mock, tbfa_mock, gsc_mock):
        def _xcom_getter(key):
            return {"uuid": "abc123"}[key]

        class _exception_formatter:
            def __init__(self, excp_str):
                self.excp_str = excp_str

            def format(self):
                return f"This is the formatted version of {self.excp_str}"

        gsc_mock.return_value = self.context_mock_value
        gat_mock.return_value = "auth_token"
        tbfa_mock.from_exception = _exception_formatter
        dag_run_mock = MagicMock(
            conf={"dryrun": False},
            dag_id="test_dag_id",
            execution_date=date.fromisoformat("2025-06-05"),
        )
        task_mock = MagicMock(task_id="mytaskid")
        task_instance_mock = MagicMock()
        task_instance_mock.xcom_pull.side_effect = _xcom_getter
        fcb = FailureCallback(__name__)
        tweaked_ctx = self.context_mock_value.copy()
        tweaked_ctx["task_instance"] = task_instance_mock
        tweaked_ctx["task"] = task_mock
        tweaked_ctx["crypt_auth_tok"] = "test_crypt_auth_tok"
        tweaked_ctx["dag_run"] = dag_run_mock
        tweaked_ctx["exception"] = "FakeTestException"
        with patch("status_change.status_manager.HttpHook.run") as hhr_mock:
            hhr_mock.return_value.json.return_value = self.good_context
            fcb(tweaked_ctx)
            args, kwargs = hhr_mock.call_args
            assert args[0] == "/entities/abc123?reindex=True"
            assert "mytaskid" in args[1]
            assert "test_dag_id" in args[1]
            assert "2025-06-05" in args[1]
            assert "mytaskid" in args[1]
            assert __name__ in args[1]
            assert "This is the formatted version of FakeTestException" in args[1]
            assert args[2]["authorization"] == "Bearer auth_token"


# if __name__ == "__main__":
#     suite = unittest.TestLoader().loadTestsFromTestCase(TestEntityUpdater)
#     suite.debug()
