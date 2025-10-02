import unittest
from datetime import date
from functools import cached_property
from unittest.mock import MagicMock, patch

from status_change.callbacks.failure_callback import FailureCallback
from status_change.data_ingest_board_manager import DataIngestBoardManager
from status_change.slack.base import SlackMessage
from status_change.slack.reorganized import (
    SlackUploadReorganized,
    SlackUploadReorganizedPriority,
)
from status_change.slack_manager import SlackManager
from status_change.status_manager import (
    EntityUpdateException,
    EntityUpdater,
    StatusChanger,
    Statuses,
)
from status_change.status_utils import (
    get_data_ingest_board_query_url,
    get_entity_id,
    get_entity_ingest_url,
    get_env,
    get_globus_url,
    get_headers,
    get_project,
    is_internal_error,
)
from utils import pythonop_set_dataset_state

from airflow.models.connection import Connection

conn_mock_hm = Connection(host=f"https://ingest.api.hubmapconsortium.org")

good_upload_context = {
    "validation_message": "existing validation_message text",
    "ingest_task": "existing ingest_task text",
    "unrelated_field": True,
    "status": "new",
    "entity_type": "Upload",
}
upload_context_mock_value = {
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
    "status": "New",
    "entity_type": "Upload",
}

dataset_context_mock_value = {
    "uuid": "test_dataset_uuid",
    "hubmap_id": "test_hm_dataset_id",
    "created_by_user_displayname": "Test User",
    "created_by_user_email": "test@user.com",
    "status": "New",
    "entity_type": "Dataset",
}


class TestEntityUpdater(unittest.TestCase):
    validation_msg = "Test validation message"
    ingest_task = "Plugins passed: test_1, test_2"

    @cached_property
    @patch("status_change.status_manager.get_submission_context")
    def upload_entity_valid(self, context_mock):
        context_mock.return_value = good_upload_context
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

    @patch("status_change.status_manager.EntityUpdater.validate_fields_to_change")
    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_update(self, conn_mock, hhr_mock, validate_mock):
        conn_mock.return_value = conn_mock_hm
        hhr_mock.assert_not_called()
        validate_mock.assert_not_called()
        self.upload_entity_valid.update()
        validate_mock.assert_called_once()
        hhr_mock.assert_called_once()

    @patch("status_change.status_manager.EntityUpdater.set_entity_api_data")
    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_manager.StatusChanger.update")
    @patch("status_change.status_manager.get_submission_context")
    def test_should_be_statuschanger(self, context_mock, sc_mock, hhr_mock, eu_mock):
        context_mock.return_value = good_upload_context
        hhr_mock.return_value.json.return_value = good_upload_context
        with_status = EntityUpdater(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"status": "valid", "validation_message": self.validation_msg},
            fields_to_append_to={"ingest_task": self.ingest_task},
        )
        sc_mock.assert_not_called()
        with_status.update()
        sc_mock.assert_called_once()
        eu_mock.assert_not_called()


class TestStatusChanger(unittest.TestCase):
    validation_msg = "Test validation message"

    @patch("status_change.status_manager.EntityUpdater.set_entity_api_data")
    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_manager.StatusChanger.set_entity_api_data")
    @patch("status_change.status_manager.get_submission_context")
    def test_should_be_entityupdater(self, context_mock, sc_mock, hhr_mock, eu_mock):
        context_mock.return_value = good_upload_context
        hhr_mock.return_value.json.return_value = good_upload_context
        without_status = StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"validation_message": self.validation_msg},
        )
        assert without_status.status == None
        assert without_status.fields_to_change
        eu_mock.assert_not_called()
        without_status.update()
        eu_mock.assert_called_once()
        sc_mock.assert_not_called()

    @patch("status_change.status_manager.StatusChanger.call_message_managers")
    @patch("status_change.status_manager.EntityUpdater.set_entity_api_data")
    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_manager.StatusChanger.set_entity_api_data")
    @patch("status_change.status_manager.get_submission_context")
    def test_same_status(self, context_mock, sc_mock, hhr_mock, eu_mock, mm_mock):
        context_mock.return_value = good_upload_context
        hhr_mock.return_value.json.return_value = good_upload_context
        same_status = StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="new",
            fields_to_overwrite={"validation_message": self.validation_msg},
        )
        assert same_status.same_status == True
        assert same_status.status == Statuses.UPLOAD_NEW
        eu_mock.assert_not_called()
        same_status.update()
        eu_mock.assert_called_once()
        sc_mock.assert_not_called()
        mm_mock.assert_called_once()

    @cached_property  # could use lru_cache with project arg to make hubmap/sennet aware
    def upload_valid(self):
        with patch("status_change.status_utils.HttpHook.get_connection") as conn_mock:
            with patch("status_change.status_manager.get_submission_context") as mock_mthd:
                conn_mock.return_value = conn_mock_hm
                mock_mthd.return_value = good_upload_context
                return StatusChanger(
                    "upload_valid_uuid",
                    "upload_valid_token",
                    status="Valid",
                    extra_options={},
                )

    @cached_property
    def upload_invalid(self):
        with patch("status_change.status_utils.HttpHook.get_connection") as conn_mock:
            with patch("status_change.status_manager.get_submission_context") as mock_mthd:
                conn_mock.return_value = conn_mock_hm
                mock_mthd.return_value = good_upload_context
                return StatusChanger(
                    "upload_valid_uuid",
                    "upload_valid_token",
                    status="Invalid",
                    extra_options={},
                )

    def test_unrecognized_status(self):
        with patch("status_change.status_manager.get_submission_context") as gsc_mock:
            gsc_mock.return_value = good_upload_context
            with self.assertRaises(EntityUpdateException):
                # No "Published" status for uploads
                StatusChanger(
                    "invalid_status_uuid",
                    "invalid_status_token",
                    status="Published",
                    extra_options={},
                )

    def test_recognized_status(self):
        self.upload_valid.validate_fields_to_change()
        self.assertEqual(
            self.upload_valid.fields_to_change["status"],
            Statuses.get_status_str(self.upload_valid.status),
        )

    def test_extra_fields(self):
        with patch("status_change.status_manager.get_submission_context") as gsc_mock:
            gsc_mock.return_value = good_upload_context
            with_extra_field = StatusChanger(
                "extra_field_uuid",
                "extra_field_token",
                status=Statuses.UPLOAD_PROCESSING,
                fields_to_overwrite={"test_extra_field": True},
            )
            data = with_extra_field.fields_to_change
            self.assertIn("test_extra_field", data)
            self.assertEqual(data["test_extra_field"], True)

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.put_request_to_entity_api")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_extra_field_good(self, conn_mock, entity_api_mock, context_mock):
        conn_mock.return_value = conn_mock_hm
        with patch("status_change.status_utils.HttpHook.run"):
            context_mock.return_value = {
                "status": "processing",
                "test_extra_field": False,
                "entity_type": "Upload",
            }
            with_extra_field = StatusChanger(
                "extra_options_uuid",
                "extra_options_token",
                status="valid",
                fields_to_overwrite={"test_extra_field": True},
                verbose=False,
            )
            with_extra_field.update()
            self.assertIn(
                {"test_extra_field": True, "status": "valid"}, entity_api_mock.call_args.args
            )

    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_valid_status_in_request(self, conn_mock, hhr_mock):
        conn_mock.return_value = conn_mock_hm
        self.upload_valid.validate_fields_to_change()
        self.upload_valid.set_entity_api_data()
        self.assertIn('{"status": "valid"}', hhr_mock.call_args.args)

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_invalid_status_in_request(self, conn_mock, ctx_mock):
        conn_mock.return_value = conn_mock_hm
        ctx_mock.return_value = {
            "status": "processing",
            "test_extra_field": True,
            "entity_type": "Upload",
        }
        with patch("status_change.status_utils.HttpHook.run"):
            sc = StatusChanger(
                "my_test_uuid",
                "my_test_token",
                status="Valid",
                extra_options={},
                fields_to_overwrite={"test_extra_field": False},
            )
            sc.validate_fields_to_change()
            sc.update()
            # patch the StatusChanger to avoid the tests in its constructor which
            # would normally detect an invalid update
            sc.fields_to_change["status"] = "published"
            with self.assertRaises((AssertionError, EntityUpdateException)):
                sc.update()

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_http_conn_id(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        with patch("status_change.status_utils.HttpHook.run") as httpr_mock:
            httpr_mock.return_value.json.return_value = good_upload_context
            with_http_conn_id = StatusChanger(
                "http_conn_uuid",
                "http_conn_token",
                status=Statuses.DATASET_NEW,
                http_conn_id="test_conn_id",
            )
            assert with_http_conn_id.http_conn_id == "test_conn_id"

    @patch("status_change.slack_manager.SlackManager.update")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_call_message_managers_valid_status(self, conn_mock, slack_mock):
        conn_mock.return_value = Connection(host=f"https://ingest.api.hubmapconsortium.org")
        self.assertFalse(slack_mock.called)
        self.assertEqual(self.upload_invalid.status, Statuses.UPLOAD_INVALID)
        with patch("status_change.slack_manager.post_to_slack_notify"):
            with patch("status_change.status_utils.HttpHook.run"):
                self.upload_invalid.update()
        self.assertTrue(slack_mock.called)

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
            reindex=True,
            dag=None,
            run_id=None,
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
            reindex=True,
            dag=None,
            run_id=None,
        )

    @patch("utils.get_auth_tok")
    def test_pythonop_set_dataset_state_invalid(self, gat_mock):
        with patch("status_change.status_utils.HttpHook.run"):
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

    @patch("status_change.slack_manager.get_submission_context")
    @patch("status_change.slack.base.get_submission_context")
    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.put_request_to_entity_api")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_triggered(
        self,
        conn_mock,
        entity_api_mock,
        sc_context_mock,
        slack_context_mock,
        slack_base_context_mock,
    ):
        conn_mock.return_value = Connection(host=f"https://ingest.api.hubmapconsortium.org")
        with patch("status_change.status_utils.HttpHook.run"):
            new_context = upload_context_mock_value
            sc_context_mock.return_value = new_context
            slack_context_mock.return_value = new_context
            slack_base_context_mock.return_value = new_context
            with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
                with patch("status_change.slack_manager.SlackManager.update") as update_mock:
                    StatusChanger(
                        "upload_valid_uuid",
                        "upload_valid_token",
                        status="Reorganized",
                        extra_options={},
                    ).update()
                    update_mock.assert_called_once()
                    entity_api_mock.assert_not_called()

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    @patch("status_change.slack_manager.SlackManager.update")
    @patch("status_change.status_manager.get_submission_context")
    def test_slack_not_triggered(self, context_mock, slack_mock, ingest_board_mock):
        with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
            context_mock.return_value = dataset_context_mock_value
            ingest_board_mock.return_value = {"entity_type": "dataset"}
            StatusChanger(
                "dataset_valid_uuid",
                "dataset_valid_token",
                status="Hold",
                extra_options={},
            ).update()
            slack_mock.assert_not_called()

    @patch("status_change.data_ingest_board_manager.DataIngestBoardManager.update")
    @patch("status_change.data_ingest_board_manager.get_submission_context")
    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_data_ingest_board_triggered(
        self, conn_mock, sc_context_mock, dib_context_mock, dib_update_mock
    ):
        conn_mock.return_value = conn_mock_hm
        with patch("status_change.status_utils.HttpHook.run"):
            sc_context_mock.return_value = upload_context_mock_value
            dib_context_mock.return_value = upload_context_mock_value
            with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
                with patch("status_change.slack_manager.get_submission_context"):
                    with patch("status_change.slack_manager.SlackManager.update"):
                        sc = StatusChanger(
                            "upload_valid_uuid",
                            "upload_valid_token",
                            status="Reorganized",
                            extra_options={},
                        )
                        sc.update()
                        dib_update_mock.assert_called_once()

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    @patch("status_change.data_ingest_board_manager.DataIngestBoardManager.update")
    @patch("status_change.status_manager.get_submission_context")
    def test_data_ingest_board_not_triggered(self, context_mock, dib_mock, ingest_board_mock):
        with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
            context_mock.return_value = dataset_context_mock_value
            ingest_board_mock.return_value = {"entity_type": "dataset"}
            StatusChanger(
                "dataset_valid_uuid",
                "dataset_valid_token",
                status="Hold",
                extra_options={},
            ).update()
            dib_mock.assert_not_called()


class SlackTest(SlackMessage):
    name = "test_class"

    def format(self):
        return "I am formatted"


class TestSlack(unittest.TestCase):

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_message_formatting(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        status = Statuses.DATASET_DEPRECATED
        with patch.object(
            SlackManager,
            "status_to_class",
            {status: {"main_class": SlackTest, "subclasses": []}},
        ):
            mgr = self.slack_manager(status, {})
        assert mgr.message_class.format() == "I am formatted"

    @patch("status_change.status_utils.get_env")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_slack_channel(self, conn_mock, env_mock):
        conn_mock.return_value = conn_mock_hm
        env_mock.return_value = "prod"
        with patch.dict(
            "status_change.slack_manager.SlackManager.status_to_class",
            {"test_status": {"main_class": SlackTest, "subclasses": []}},
        ):
            with patch.dict(
                "status_change.status_utils.slack_channels", {"test_class": "test_channel"}
            ):
                mgr = self.slack_manager("test_status", {})
        assert mgr.message_class.channel == "test_channel"

    @patch("status_change.slack_manager.get_env")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_update_with_slack_channel(self, conn_mock, env_mock):
        """
        We expect to see prod environment use value in `slack_channels`,
        and non-prod envs to replace channel value with the
        value from `slack_channels_testing`.
        """
        conn_mock.return_value = conn_mock_hm
        with patch.dict(
            "status_change.status_utils.slack_channels",
            {"base": "base_channel", "test_class": "test_channel"},
        ):
            with patch.dict(
                "status_change.status_utils.slack_channels_testing",
                {"test_class": "base_channel"},
            ):
                with patch.dict(
                    "status_change.slack_manager.SlackManager.status_to_class",
                    {Statuses.UPLOAD_INVALID: {"main_class": SlackTest, "subclasses": []}},
                ):
                    for env_val, channel in {
                        "prod": "test_channel",
                        "dev": "base_channel",
                    }.items():
                        env_mock.return_value = env_val
                        mgr = self.slack_manager(Statuses.UPLOAD_INVALID, good_upload_context)
                        with patch(
                            "status_change.slack_manager.post_to_slack_notify"
                        ) as slack_mock:
                            mgr.update()
                            slack_mock.assert_called_once_with(
                                "test_token", "I am formatted", channel
                            )

    @patch("status_change.slack_manager.get_env")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_update_with_slack_channel_not_found(self, conn_mock, env_mock):
        """
        We expect to see "base" channel if channel not found for status
        in all environments.
        """
        conn_mock.return_value = conn_mock_hm
        with patch.dict(
            "status_change.status_utils.slack_channels",
            {"base": "base_channel"},
        ):
            with patch.dict(
                "status_change.status_utils.slack_channels_testing",
                {"base": "base_channel"},
            ):
                with patch.dict(
                    "status_change.slack_manager.SlackManager.status_to_class",
                    {Statuses.DATASET_HOLD: {"main_class": SlackTest, "subclasses": []}},
                ):
                    for env_val, channel in {
                        "prod": "base_channel",
                        "dev": "base_channel",
                    }.items():
                        env_mock.return_value = env_val
                        mgr = self.slack_manager(Statuses.DATASET_HOLD, good_upload_context)
                        with patch(
                            "status_change.slack_manager.post_to_slack_notify"
                        ) as slack_mock:
                            mgr.update()
                            slack_mock.assert_called_once_with(
                                "test_token", "I am formatted", channel
                            )

    @patch("status_change.slack.base.get_submission_context")
    @patch("status_change.slack_manager.get_submission_context")
    def slack_manager(self, status, context, context_mock, slack_mock):
        context_mock.return_value = context
        slack_mock.return_value = context
        with patch("status_change.slack_manager.SlackManager.update"):
            return SlackManager(status, "test_uuid", "test_token")

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_manager_main_class(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED, good_upload_context)
        assert type(mgr.message_class) is SlackUploadReorganized

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_manager_subclass(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        context_copy = good_upload_context.copy()
        context_copy.update({"priority_project_list": ["PRIORITY"]})
        mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED, context_copy)
        assert type(mgr.message_class) is SlackUploadReorganizedPriority

    @patch("status_change.slack_manager.SlackManager.update")
    def test_slack_manager_no_rule(self, update_mock):
        mgr = SlackManager(Statuses.DATASET_DEPRECATED, "test_uuid", "test_token")
        assert not mgr.is_valid_for_status
        update_mock.assert_not_called()


class TestFailureCallback(unittest.TestCase):
    @patch("utils.airflow_conf.as_dict")
    @patch("status_change.status_utils.get_submission_context")
    @patch("traceback.TracebackException.from_exception")
    @patch("status_change.callbacks.base.get_auth_tok")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_failure_callback(self, conn_mock, gat_mock, tbfa_mock, gsc_mock, af_conf_mock):
        conn_mock.return_value = conn_mock_hm
        af_conf_mock.return_value = {"connections": {"WORKFLOW_SCRATCH": "/scratch/path"}}

        def _xcom_getter(key):
            return {"uuid": "abc123"}[key]

        class _exception_formatter:
            def __init__(self, excp_str):
                self.excp_str = excp_str

            def format(self):
                return f"This is the formatted version of {self.excp_str}"

        gsc_mock.return_value = upload_context_mock_value
        gat_mock.return_value = "auth_token"
        tbfa_mock.side_effect = lambda excp: _exception_formatter(excp)
        dag_run_mock = MagicMock(
            conf={"dryrun": False},
            dag_id="test_dag_id",
            execution_date=date.fromisoformat("2025-06-05"),
        )
        task_mock = MagicMock(task_id="mytaskid")
        task_instance_mock = MagicMock()
        task_instance_mock.xcom_pull.side_effect = _xcom_getter
        with patch("status_change.slack_manager.SlackManager.update"):
            with patch("status_change.data_ingest_board_manager.DataIngestBoardManager.update"):
                fcb = FailureCallback(__name__)
                tweaked_ctx = upload_context_mock_value.copy()
                tweaked_ctx["task_instance"] = task_instance_mock
                tweaked_ctx["task"] = task_mock
                tweaked_ctx["crypt_auth_tok"] = "test_crypt_auth_tok"
                tweaked_ctx["dag_run"] = dag_run_mock
                tweaked_ctx["exception"] = "FakeTestException"
                with patch("status_change.status_utils.HttpHook.run") as hhr_mock:
                    hhr_mock.return_value.json.return_value = good_upload_context
                    fcb(tweaked_ctx)
                    assert "mytaskid" == fcb.task.task_id
                    assert "test_dag_id" == fcb.dag_run.dag_id
                    assert date(2025, 6, 5) == fcb.dag_run.execution_date
                    assert __name__ == fcb.called_from
                    assert (
                        "This is the formatted version of FakeTestException"
                        == fcb.formatted_exception
                    )
                    # mocking HttpHook.run in status_utils means there are a lot of calls,
                    # need to find the call with the error info (i.e. the PUT call)
                    hhr_call = None
                    for call in hhr_mock.call_args_list:
                        for call_tuple in call:
                            for call_arg in call_tuple:
                                if "error_message" in call_arg:
                                    hhr_call = call
                    assert hhr_call
                    assert hhr_call[0][0] == "/entities/abc123?reindex=True"
                    assert hhr_call[0][2]["authorization"] == "Bearer auth_token"


class TestDataIngestBoardManager(unittest.TestCase):

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    def test_clear_status(self, dib_context_mock):
        dib_context_mock.return_value = upload_context_mock_value
        dib = DataIngestBoardManager(Statuses.UPLOAD_VALID, "test_uuid", "test_token")
        assert dib.get_fields() == {"error_message": ""}

    @patch("status_change.data_ingest_board_manager.DataIngestBoardManager.upload_invalid")
    @patch("status_change.data_ingest_board_manager.DataIngestBoardManager.update")
    @patch("status_change.data_ingest_board_manager.get_submission_context")
    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_valid_status_from_statuschanger(
        self, conn_mock, sc_context_mock, dib_context_mock, dib_update_mock, upload_invalid_mock
    ):
        conn_mock.return_value = conn_mock_hm
        with patch("status_change.status_utils.HttpHook.run"):
            sc_context_mock.return_value = upload_context_mock_value
            dib_context_mock.return_value = upload_context_mock_value
            with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
                with patch("status_change.slack_manager.get_submission_context"):
                    with patch("status_change.slack_manager.SlackManager.update"):
                        sc = StatusChanger(
                            "upload_valid_uuid",
                            "upload_valid_token",
                            status="Invalid",
                            extra_options={},
                        )
                        sc.update()
                        dib_update_mock.assert_called_once()
                        upload_invalid_mock.assert_called_once()

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    def test_valid_status_return_ext_error(self, dib_context_mock):
        dib_context_mock.return_value = upload_context_mock_value
        dib = DataIngestBoardManager(
            Statuses.UPLOAD_INVALID, "test_uuid", "test_token", run_id="test_run_id"
        )
        assert dib.get_fields() == {"error_message": f"Invalid status from run test_run_id"}

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    def test_valid_status_return_int_error(self, dib_context_mock):
        context_with_int_error = upload_context_mock_value.copy()
        dib_context_mock.return_value = context_with_int_error | {
            "error_message": "Internal error--test"
        }
        with patch.object(DataIngestBoardManager, "log_directory_path", "test_log"):
            dib = DataIngestBoardManager(
                Statuses.UPLOAD_INVALID, "test_uuid", "test_token", run_id="test_run_id"
            )
            assert dib.get_fields() == {
                "error_message": "Internal error. Log directory: test_log.",
                "assigned_to_group_name": "IEC Testing Group",
            }
            dib_w_msg = DataIngestBoardManager(
                Statuses.UPLOAD_INVALID,
                "test_uuid",
                "test_token",
                run_id="test_run_id",
                msg="test message!",
            )
            assert dib_w_msg.get_fields() == {
                "error_message": f"Internal error. Log directory: test_log. test message!",
                "assigned_to_group_name": "IEC Testing Group",
            }

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    def test_status_invalid_for_manager(self, dib_context_mock):
        dib_context_mock.return_value = upload_context_mock_value
        dib = DataIngestBoardManager(Statuses.DATASET_HOLD, "test_uuid", "test_token")
        assert dib.get_fields() == None
        assert dib.is_valid_for_status == False

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    def test_get_msg(self, dib_context_mock):
        msg = "Antibodies/Contributors Errors: 1"
        dib_context_mock.return_value = upload_context_mock_value
        dib = DataIngestBoardManager(Statuses.UPLOAD_INVALID, "test_uuid", "test_token", msg=msg)
        assert dib.get_fields() == {"error_message": msg}


class TestStatusUtils(unittest.TestCase):
    hm_entity_data = {"hubmap_id": "test_hubmap_id"}
    sn_entity_data = {"sennet_id": "test_sennet_id"}

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_project(self, conn_mock):
        for ingest_context in ["hubmap", "sennet"]:
            conn_mock.return_value = Connection(
                host=f"https://ingest.api.{ingest_context}consortium.org"
            )
            proj = get_project()
            assert proj.value[0] == ingest_context

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_entity_id(self, conn_mock):
        for ingest_context, entity_data in {
            "hubmap": self.hm_entity_data,
            "sennet": self.sn_entity_data,
        }.items():
            conn_mock.return_value = Connection(
                host=f"https://ingest.api.{ingest_context}consortium.org"
            )
            entity_id = get_entity_id(entity_data)
            assert entity_id == f"test_{ingest_context}_id"

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_headers(self, conn_mock):
        for proj in ["hubmap", "sennet"]:
            conn_mock.return_value = Connection(host=f"https://ingest.api.{proj}consortium.org")
            assert get_headers("test_token") == {
                "authorization": f"Bearer test_token",
                "content-type": "application/json",
                f"X-{proj.title()}-Application": "ingest-pipeline",
            }

    def test_is_internal_error(self):
        entity_data_dicts = [
            ({"error_message": None, "status": "new"}, False),
            ({"error_message": None, "status": "error"}, True),
            ({"status": "error"}, True),
            ({"status": "invalid"}, False),
            ({"error_message": "Internal error: test error", "status": "invalid"}, True),
            ({"error_message": "Directory errors: 5", "status": "invalid"}, False),
        ]

        for entity_data, is_error in entity_data_dicts:
            assert is_internal_error(entity_data) == is_error, f"{entity_data} is not {is_error}"

    endpoints = {
        "hubmap": {
            "PROD": {"entity_url": "https://entity.api.hubmapconsortium.org"},
            "DEV": {"entity_url": "https://entity-api.dev.hubmapconsortium.org"},
        },
        "sennet": {
            "PROD": {"entity_url": "https://entity.api.sennetconsortium.org"},
            "DEV": {"entity_url": "https://entity-api.dev.sennetconsortium.org"},
        },
    }

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_env(self, conn_mock):
        for proj in ["hubmap", "sennet"]:
            with patch("utils.ENDPOINTS", self.endpoints[proj]):
                for env, host_url in {
                    "prod": f"https://entity.api.{proj}consortium.org",
                    "dev": f"https://entity-api.dev.{proj}consortium.org",
                    None: "https://badurl",
                }.items():
                    conn_mock.return_value = Connection(host=host_url)
                    assert get_env() == env

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_entity_ingest_url(self, hhr_mock):
        for url_prefix, entity_data in {
            "https://ingest-api.dev.hubmapconsortium.org": {
                "entity_type": "upload",
                "uuid": "test_hm_uuid",
            },
            "https://ingest.api.sennetconsortium.org": {
                "entity_type": "dataset",
                "uuid": "test_sn_uuid",
            },
        }.items():
            hhr_mock.return_value = Connection(host=url_prefix)
            url = get_entity_ingest_url(entity_data)
            expected_url = f"{url_prefix}/{entity_data['entity_type']}/{entity_data['uuid']}"
            print(f"assert {url} == {expected_url}")
            assert url == expected_url

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_data_ingest_board_query_url(self, hhr_mock):
        for proj in ["hubmap", "sennet"]:
            with patch("utils.ENDPOINTS", self.endpoints[proj]):
                for url_prefix, ingest_conn, entity_data in [
                    (
                        f"https://ingest-board.dev.{proj}consortium.org",
                        f"https://entity-api.dev.{proj}consortium.org",
                        {
                            "entity_type": "upload",
                            "uuid": "test_dev_uuid",
                            f"{proj}_id": "test_dev_id",
                        },
                    ),
                    (
                        f"https://ingest.board.{proj}consortium.org",
                        f"https://entity.api.{proj}consortium.org",
                        {
                            "entity_type": "dataset",
                            "uuid": "test_prod_uuid",
                            f"{proj}_id": "test_prod_id",
                        },
                    ),
                ]:
                    hhr_mock.return_value = Connection(host=ingest_conn)
                    url = get_data_ingest_board_query_url(entity_data)
                    entity_id = entity_data.get(f"{get_project().value[0]}_id")
                    expected_url = f"{url_prefix}/?q={entity_id}"
                    if entity_data["entity_type"] == "upload":

                        expected_url += "&entity_type=uploads"
                    print(f"assert {url} == {expected_url}")
                    assert url == expected_url

    @patch("status_change.status_utils.get_abs_path")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_globus_url(self, conn_mock, abs_path_mock):
        test_data = {
            "sn_public_uuid": {
                "sennet": {
                    "abs_path": "/codcc-dev/data/public/sn_public_uuid",
                    "dest_path": "https://app.globus.org/file-manager?origin_id=96b2b9e5-6915-4dbc-9ab5-173ad628902e&origin_path=sn_public_uuid",
                }
            },
            "hm_public_uuid": {
                "hubmap": {
                    "abs_path": "/hive/hubmap-dev/data/public/hm_public_uuid",
                    "dest_path": "https://app.globus.org/file-manager?origin_id=af603d86-eab9-4eec-bb1d-9d26556741bb&origin_path=hm_public_uuid",
                }
            },
            "sn_protected_uuid": {
                "sennet": {
                    "abs_path": "/codcc-prod/data/protected/component/sn_protected_uuid",
                    "dest_path": "https://app.globus.org/file-manager?origin_id=45617036-f2cc-4320-8108-edf599290158&origin_path=%2Fprotected%2Fcomponent%2Fsn_protected_uuid%2F",
                }
            },
            "hm_protected_uuid": {
                "hubmap": {
                    "abs_path": "/hive/hubmap/data/protected/component/hm_protected_uuid",
                    "dest_path": "https://app.globus.org/file-manager?origin_id=24c2ee95-146d-4513-a1b3-ac0bfdb7856f&origin_path=%2Fprotected%2Fcomponent%2Fhm_protected_uuid%2F",
                }
            },
        }
        for uuid, data in test_data.items():
            for proj, paths in data.items():
                with patch("utils.ENDPOINTS", self.endpoints[proj]):
                    abs_path_mock.return_value = paths["abs_path"]
                    conn_mock.return_value = Connection(
                        host=f"https://entity.api.{proj}consortium.org"
                    )
                    print(get_globus_url(uuid, "test_token"))
                    assert get_globus_url(uuid, "test_token") == paths["dest_path"]


# if __name__ == "__main__":
#     suite = unittest.TestLoader().loadTestsFromTestCase(TestEntityUpdater)
#     suite.debug()
