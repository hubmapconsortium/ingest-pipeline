# pyright: reportMissingImports=false

import json
import unittest
from datetime import date
from functools import cached_property
from typing import Optional
from unittest.mock import MagicMock, PropertyMock, call, patch

import requests
from status_change.callbacks.failure_callback import FailureCallback
from status_change.data_ingest_board_manager import DataIngestBoardManager
from status_change.email_manager import EmailManager
from status_change.email_templates.invalid import InvalidStatusEmail
from status_change.slack.base import SlackMessage
from status_change.slack.error import (
    SlackDatasetError,
    SlackDatasetErrorDerived,
    SlackDatasetErrorPrimaryPipeline,
    SlackUploadError,
)
from status_change.slack.invalid import SlackDatasetInvalid, SlackUploadInvalid
from status_change.slack.new import SlackDatasetNew, SlackDatasetNewDerived
from status_change.slack.qa import SlackDatasetQA, SlackDatasetQADerived
from status_change.slack.reorganized import (
    SlackUploadReorganized,
    SlackUploadReorganizedNoDatasets,
    SlackUploadReorganizedPriority,
)
from status_change.slack_manager import SlackManager
from status_change.status_manager import (
    EntityUpdateException,
    EntityUpdater,
    StatusChanger,
    Statuses,
    call_message_managers,
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
from tests.fixtures import (
    dataset_context_mock_value,
    dataset_context_mock_value_with_error,
    derived_dataset_context_mock_value,
    endpoints,
    ext_error,
    good_upload_context,
    slack_upload_reorg_priority_str,
    slack_upload_reorg_str,
    validation_error_dict,
    validation_report_html_list,
)
from utils import pythonop_set_dataset_state

from airflow.configuration import conf as airflow_conf
from airflow.models.connection import Connection

conn_mock_hm = Connection(host=f"https://ingest.api.hubmapconsortium.org")


def get_mock_response(good: bool, response_data: bytes) -> requests.Response:
    mock_resp = requests.models.Response()
    mock_resp.url = "test_url"
    mock_resp.status_code = 200 if good else 400
    mock_resp.reason = "OK" if good else "Bad Request"
    mock_resp._content = response_data
    return mock_resp


class MockParent(unittest.TestCase):
    """
    The status change functionality is incredibly dependent on
    other APIs (GET/PUT to entity-api, POST to Slack notifier, email)
    and configs (airflow_conf). This parent class does its best to
    turn off all outside calls by default.

    It contains patches at various levels. e.g. HttpHook represents an
    actual API call but can use different HTTP methods, whereas
    set_entity_api_data is the method where HttpHook is called to
    specifically make a PUT request.

    Test classes are subclasses of MockParent. They can override as
    necessary in a setUp method and/or in individual test methods,
    e.g. to provide a specific return_value.

    Patches may also need to be turned off as some are nested, and
    it may be desirable to test functionality inside of a patched method.
    E.g. a test that asserts self.mock_httphook.assert_called_once()
    will fail unless the patch affecting the method where it is called
    is turned off (e.g. self.entity_update.stop() - note that this references
    the *patch* attribute rather than the mock).

    Other patches may be necessary at the subclass or test level.
    """

    def setUp(self):

        # Patch objects
        self.token = patch("utils.get_auth_tok", return_value="test_token")
        self.conn = patch(
            "status_change.status_utils.HttpHook.get_connection",
            return_value=Connection(host=f"https://ingest.api.hubmapconsortium.org"),
        )
        self.env = patch("status_change.slack_manager.get_env", side_effect=["prod", "dev"])
        self.conf = patch(
            "utils.airflow_conf.as_dict",
            return_value={"connections": {"WORKFLOW_SCRATCH": "test_path"}},
        )
        self.httphook = patch("status_change.status_utils.HttpHook.run")
        self.get_ancestors = patch("status_change.status_utils.get_ancestors")
        # self.scratch_base_path = patch("utils._get_scratch_base_path", return_value="test_path")
        self.entity_update = patch("status_change.status_manager.put_request_to_entity_api")
        self.status_context = patch(
            "status_change.status_utils.get_submission_context", return_value=good_upload_context
        )
        self.slack_msg_context = patch(
            "status_change.slack.base.get_submission_context", return_value=good_upload_context
        )
        self.slack_update = patch("status_change.slack_manager.SlackManager.update")
        self.slack_post = patch("utils.post_to_slack_notify")
        self.dib_update = patch(
            "status_change.data_ingest_board_manager.DataIngestBoardManager.update"
        )
        self.email_send = patch("status_change.email_manager.EmailManager.send_email")

        # Mock objects
        self.mock_token = self.token.start()
        self.mock_conn = self.conn.start()
        self.mock_env = self.env.start()
        self.mock_conf = self.conf.start()
        self.mock_httphook = self.httphook.start()
        self.mock_httphook.return_value.json.return_value = good_upload_context  # GET request mock
        self.mock_get_ancestors = self.get_ancestors.start()
        self.mock_entity_update = self.entity_update.start()
        self.mock_status_context = self.status_context.start()
        self.mock_slack_msg_context = self.slack_msg_context.start()
        self.mock_slack_update = self.slack_update.start()
        self.mock_slack_post = self.slack_post.start()
        self.mock_dib_update = self.dib_update.start()
        self.mock_email_send = self.email_send.start()

        # Stop/clean up patches after test
        self.addCleanup(self.token.stop)
        self.addCleanup(self.conn.stop)
        self.addCleanup(self.env.stop)
        self.addCleanup(self.conf.stop)
        self.addCleanup(self.httphook.stop)
        self.addCleanup(self.get_ancestors.stop)
        self.addCleanup(self.entity_update.stop)
        self.addCleanup(self.status_context.stop)
        self.addCleanup(self.slack_msg_context.stop)
        self.addCleanup(self.slack_update.stop)
        self.addCleanup(self.slack_post.stop)
        self.addCleanup(self.dib_update.stop)
        self.addCleanup(self.email_send.stop)

        super().setUp()


class TestEntityUpdater(MockParent):
    validation_msg = "Test validation message"
    ingest_task = "Plugins passed: test_1, test_2"

    @cached_property
    def upload_entity_valid(self):
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
    def test_update(self, validate_mock):
        self.entity_update.stop()
        validate_mock.assert_not_called()
        self.upload_entity_valid.update()
        validate_mock.assert_called_once()
        put_calls = []
        for call in self.mock_httphook.call_args_list:
            for call_tuple in call:
                for call_arg in call_tuple:
                    if call_arg == "/entities/upload_valid_uuid?reindex=True":
                        put_calls.append(call)
        assert len(put_calls) == 1

    @patch("status_change.status_manager.StatusChanger.set_entity_api_data")
    def test_should_be_statuschanger(self, status_update_mock):
        with_status = EntityUpdater(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"status": "valid", "validation_message": self.validation_msg},
            fields_to_append_to={"ingest_task": self.ingest_task},
        )
        status_update_mock.assert_not_called()
        with_status.update()
        status_update_mock.assert_called_once()
        self.mock_entity_update.assert_not_called()


class TestStatusChanger(MockParent):
    validation_msg = "Test validation message"

    @patch("status_change.status_manager.StatusChanger.set_entity_api_data")
    def test_should_be_entityupdater(self, status_update_mock):
        without_status = StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"validation_message": self.validation_msg},
        )
        assert without_status.status == None
        assert without_status.fields_to_change
        self.mock_entity_update.assert_not_called()
        without_status.update()
        self.mock_entity_update.assert_called_once()
        status_update_mock.assert_not_called()

    @patch("status_change.status_manager.StatusChanger.set_entity_api_data")
    @patch("status_change.status_manager.call_message_managers")
    def test_same_status(self, mm_mock, status_update_mock):
        same_status = StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="new",
            fields_to_overwrite={"validation_message": self.validation_msg},
        )
        assert same_status.same_status == True
        assert same_status.status == Statuses.UPLOAD_NEW
        self.mock_entity_update.assert_not_called()
        same_status.update()
        self.mock_entity_update.assert_called_once()
        status_update_mock.assert_not_called()
        mm_mock.assert_called_once()

    @cached_property  # could use lru_cache with project arg to make hubmap/sennet aware
    def upload_valid(self):
        return StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="Valid",
            extra_options={},
        )

    @cached_property
    def upload_invalid(self):
        return StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="Invalid",
            extra_options={},
        )

    def test_unrecognized_status(self):
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
        assert self.upload_valid.status
        self.assertEqual(
            self.upload_valid.fields_to_change["status"],
            Statuses.valid_str(self.upload_valid.status),
        )

    def test_extra_fields(self):
        with_extra_field = StatusChanger(
            "extra_field_uuid",
            "extra_field_token",
            status=Statuses.UPLOAD_PROCESSING,
            fields_to_overwrite={"test_extra_field": True},
        )
        data = with_extra_field.fields_to_change
        self.assertIn("test_extra_field", data)
        self.assertEqual(data["test_extra_field"], True)

    def test_extra_field_good(self):
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value={
                "status": "processing",
                "test_extra_field": False,
                "entity_type": "Upload",
            },
        ):
            with_extra_field = StatusChanger(
                "extra_options_uuid",
                "extra_options_token",
                status="valid",
                fields_to_overwrite={"test_extra_field": True},
                verbose=False,
            )
            with_extra_field.update()
            self.assertIn(
                {"test_extra_field": True, "status": "valid"},
                self.mock_entity_update.call_args.args,
            )

    def test_valid_status_in_request(self):
        self.entity_update.stop()
        self.upload_valid.validate_fields_to_change()
        self.upload_valid.set_entity_api_data()
        self.assertIn('{"status": "valid"}', self.mock_httphook.call_args.args)

    def test_invalid_status_in_request(self):
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value={
                "status": "processing",
                "test_extra_field": True,
                "entity_type": "Upload",
            },
        ):
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

    def test_http_conn_id(self):
        with_http_conn_id = StatusChanger(
            "http_conn_uuid",
            "http_conn_token",
            status=Statuses.DATASET_NEW,
            http_conn_id="test_conn_id",
        )
        assert with_http_conn_id.http_conn_id == "test_conn_id"

    def test_call_message_managers_valid_status(self):
        self.assertFalse(self.mock_status_context.called)
        self.assertEqual(self.upload_invalid.status, Statuses.UPLOAD_INVALID)
        self.upload_invalid.update()
        self.assertTrue(self.mock_status_context.called)

    @staticmethod
    def my_callable(**kwargs):
        return kwargs["uuid"]

    @patch("status_change.status_manager.StatusChanger")
    def test_pythonop_set_dataset_state_valid(self, sc_mock):
        uuid = "test_uuid"
        token = "test_token"
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
            messages={"run_id": None, "processing_pipeline": None},
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
            messages={"run_id": None, "processing_pipeline": None},
        )

    def test_pythonop_set_dataset_state_invalid(self):
        uuid = "test_uuid"
        message = "Test message"
        # Pass an invalid ds_state
        with self.assertRaises(Exception):
            pythonop_set_dataset_state(
                crypt_auth_tok="test_token",
                dataset_uuid_callable=self.my_callable,
                uuid=uuid,
                message=message,
                ds_state="Unknown",
            )

    def test_slack_triggered(
        self,
    ):
        StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="Reorganized",
            extra_options={},
        ).update()
        self.mock_slack_update.assert_called_once()

    def test_slack_not_triggered(self):
        with patch(
            "status_change.status_manager.get_submission_context",
            return_value=dataset_context_mock_value,
        ):
            StatusChanger(
                "dataset_valid_uuid",
                "dataset_valid_token",
                status="Hold",
                extra_options={},
            ).update()
            self.mock_slack_update.assert_not_called()

    def test_data_ingest_board_triggered(self):
        sc = StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="Reorganized",
            extra_options={},
        )
        sc.update()
        self.mock_dib_update.assert_called_once()

    def test_data_ingest_board_not_triggered(self):
        with patch(
            "status_change.status_manager.get_submission_context",
            return_value=dataset_context_mock_value,
        ):
            StatusChanger(
                "dataset_valid_uuid",
                "dataset_valid_token",
                status="Hold",
                extra_options={},
            ).update()
            self.mock_dib_update.assert_not_called()

    def test_pass_message_managers(self):
        with patch(
            "status_change.status_manager.SlackManager.get_message_class",
            new_callable=PropertyMock,
        ) as mock_slack_mgr:
            with patch(
                "status_change.status_manager.DataIngestBoardManager.is_valid_for_status",
                new_callable=PropertyMock,
            ) as mock_dib_mgr:
                with patch(
                    "status_change.status_manager.EmailManager.is_valid_for_status",
                    new_callable=PropertyMock,
                ) as mock_email_mgr:
                    with patch(
                        "status_change.status_manager.StatisticsManager.is_valid_for_status",
                        new_callable=PropertyMock,
                    ) as mock_stats_mgr:
                        call_message_managers(
                            Statuses.DATASET_HOLD,
                            "test_uuid",
                            "test_tok",
                            messages={"processing_pipeline": "test_pipeline"},
                            message_classes=[SlackManager, DataIngestBoardManager],
                        )
                        mock_slack_mgr.assert_called_once()
                        mock_dib_mgr.assert_called_once()
                        mock_email_mgr.assert_not_called()
                        mock_stats_mgr.assert_not_called()

    def test_pass_message_managers_as_strs(self):
        with patch(
            "status_change.status_manager.SlackManager.get_message_class",
            new_callable=PropertyMock,
        ) as mock_slack_mgr:
            with patch(
                "status_change.status_manager.DataIngestBoardManager.is_valid_for_status",
                new_callable=PropertyMock,
            ) as mock_dib_mgr:
                with patch(
                    "status_change.status_manager.EmailManager.is_valid_for_status",
                    new_callable=PropertyMock,
                ) as mock_email_mgr:
                    with patch(
                        "status_change.status_manager.StatisticsManager.is_valid_for_status",
                        new_callable=PropertyMock,
                    ) as mock_stats_mgr:
                        call_message_managers(
                            Statuses.DATASET_HOLD,
                            "test_uuid",
                            "test_tok",
                            messages={"processing_pipeline": "test_pipeline"},
                            message_classes=["SlackManager", "DataIngestBoardManager"],
                        )
                        mock_slack_mgr.assert_called_once()
                        mock_dib_mgr.assert_called_once()
                        mock_email_mgr.assert_not_called()
                        mock_stats_mgr.assert_not_called()

    def test_pass_no_message_managers(self):
        with patch(
            "status_change.status_manager.SlackManager.get_message_class",
            new_callable=PropertyMock,
        ) as mock_slack_mgr:
            with patch(
                "status_change.status_manager.DataIngestBoardManager.is_valid_for_status",
                new_callable=PropertyMock,
            ) as mock_dib_mgr:
                with patch(
                    "status_change.status_manager.EmailManager.is_valid_for_status",
                    new_callable=PropertyMock,
                ) as mock_email_mgr:
                    with patch(
                        "status_change.status_manager.StatisticsManager.is_valid_for_status",
                        new_callable=PropertyMock,
                    ) as mock_stats_mgr:
                        call_message_managers(
                            Statuses.DATASET_HOLD,
                            "test_uuid",
                            "test_tok",
                            messages={"processing_pipeline": "test_pipeline"},
                        )
                        mock_slack_mgr.assert_called_once()
                        mock_dib_mgr.assert_called_once()
                        mock_email_mgr.assert_called_once()
                        mock_stats_mgr.assert_called_once()


class SlackTest(SlackMessage):
    name = "test_class"

    def format(self):
        return ["I am formatted"]


class SlackTestHold(SlackMessage):
    name = "dataset_hold"

    @classmethod
    def test(cls, entity_data, token, **kwargs):
        del token, kwargs
        if str(entity_data.get("status")).lower() == "hold":
            return True
        return False

    def format(self):
        return ["HOLD"]


@patch.dict(
    "status_change.status_utils.slack_channels",
    {"base": "base_channel", "test_class": "test_class_channel"},
)
@patch.dict(
    "status_change.status_utils.slack_channels_testing",
    {"base": "base_channel_testing", "test_class": "test_class_channel_testing"},
)
class TestSlack(MockParent):
    mock_slack_channels = {"main_class": SlackTest, "subclasses": [SlackTestHold]}

    def test_slack_message_formatting(self):
        status = Statuses.DATASET_DEPRECATED
        with patch.dict(
            "status_change.slack_manager.SlackManager.status_to_class",
            {status: self.mock_slack_channels},
        ):
            mgr = self.slack_manager(status)
        assert mgr.message_class
        assert mgr.message_class.format() == ["I am formatted"]

    def test_get_slack_channel(self):
        with patch(
            "status_change.status_utils.MessageManager.get_status", return_value="test_status"
        ):
            with patch.dict(
                "status_change.slack_manager.SlackManager.status_to_class",
                {"test_status": self.mock_slack_channels},
            ):
                mgr = self.slack_manager("test_status")
            assert mgr.message_class
            assert mgr.message_class.channel == "test_class_channel"

    def test_update_with_slack_channel(self):
        """
        We expect to see prod environment use value in `slack_channels`,
        and non-prod envs to replace channel value with the
        value from `slack_channels_testing`.
        """
        self.slack_update.stop()
        with patch.dict(
            "status_change.slack_manager.SlackManager.status_to_class",
            {Statuses.UPLOAD_INVALID: self.mock_slack_channels},
        ):
            for channel in [
                "test_class_channel",  # prod
                "test_class_channel_testing",  # dev
            ]:
                mgr = self.slack_manager(Statuses.UPLOAD_INVALID)
                mgr.update()
                self.mock_slack_post.assert_called_once_with(
                    "test_token", "I am formatted", channel
                )
                self.mock_slack_post.reset_mock()

    def test_update_with_slack_channel_not_found(self):
        """
        We expect to see "base" channel if channel not found for status
        in all environments.
        """
        self.slack_update.stop()
        original_ret_val = self.mock_status_context.return_value.copy()
        new_ret_val = original_ret_val | {"status": "hold"}

        with patch("status_change.status_utils.get_submission_context", return_value=new_ret_val):
            with patch.dict(
                "status_change.slack_manager.SlackManager.status_to_class",
                {Statuses.DATASET_HOLD: self.mock_slack_channels},
            ):
                for channel in [
                    "base_channel",  # prod
                    "base_channel_testing",  # dev
                ]:
                    mgr = self.slack_manager(Statuses.DATASET_HOLD)
                    mgr.update()
                    self.mock_slack_post.assert_called_once_with("test_token", "HOLD", channel)
                    self.mock_slack_post.reset_mock()

    def slack_manager(self, status, **kwargs):
        return SlackManager(status, "test_uuid", "test_token", **kwargs)

    def test_slack_manager_main_class(self):
        self.mock_status_context.return_value.pop("priority_project_list", None)
        self.mock_slack_msg_context.return_value.pop("priority_project_list", None)
        mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED)
        assert type(mgr.message_class) is SlackUploadReorganized

    def test_slack_manager_subclass(self):
        context_addition = {
            "priority_project_list": ["PRIORITY"],
            "datasets": [{"dataset_type": "test_1"}],
        }
        self.mock_status_context.return_value.update(context_addition)
        self.mock_slack_msg_context.return_value.update(context_addition)
        mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED)
        assert type(mgr.message_class) is SlackUploadReorganizedPriority

    def test_slack_manager_subclass_pass(self):
        self.mock_status_context.return_value.pop("datasets", None)
        self.mock_slack_msg_context.return_value.pop("datasets", None)
        mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED)
        assert type(mgr.message_class) is SlackUploadReorganizedNoDatasets
        with self.assertRaises(NotImplementedError):
            mgr.message_class.format()
        assert self.mock_slack_post.no_calls()

    def test_slack_manager_no_rule(self):
        mgr = SlackManager(Statuses.DATASET_DEPRECATED, "test_uuid", "test_token")
        assert not mgr.is_valid_for_status
        self.mock_slack_update.assert_not_called()

    @staticmethod
    def hhr_mock_side_effect(endpoint, headers, *args, **kwargs):
        del headers, args, kwargs
        if "organs" in endpoint:
            return get_mock_response(
                True, json.dumps(good_upload_context.get("datasets")).encode("utf-8")
            )
        if "file-system-abs-path" in endpoint:
            return get_mock_response(True, json.dumps({"path": "test_abs_path"}).encode("utf-8"))
        return get_mock_response(True, json.dumps(good_upload_context).encode("utf-8"))

    def test_reorganized_formatting(self):
        with patch("status_change.slack.reorganized.get_organ", return_value="test_organ"):
            with patch(
                "status_change.slack.reorganized.get_globus_url", return_value="test_globus_url"
            ):
                with patch(
                    "status_change.slack.reorganized.get_abs_path", return_value="test_abs_path"
                ):
                    self.entity_update.stop()
                    for klass, ret_val in {
                        SlackUploadReorganized: slack_upload_reorg_str,
                        SlackUploadReorganizedPriority: slack_upload_reorg_priority_str,
                    }.items():
                        with patch("utils.ENDPOINTS", endpoints["hubmap"]):
                            mgr = klass("test_uuid", "test_token")
                            assert mgr.format() == ret_val

    def test_error_classes(self):
        upload_mgr = self.slack_manager(Statuses.UPLOAD_ERROR)
        assert type(upload_mgr.message_class) is SlackUploadError
        self.status_context.stop()
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=dataset_context_mock_value,
        ):
            dataset_mgr = self.slack_manager(Statuses.DATASET_ERROR)
            assert type(dataset_mgr.message_class) is SlackDatasetError
            primary_dataset_mgr = self.slack_manager(
                Statuses.DATASET_ERROR,
                **{
                    "messages": {
                        "primary_dataset_uuid": "test_primary_uuid",
                        "processing_pipeline": "test_pipeline",
                    }
                },
            )
            assert type(primary_dataset_mgr.message_class) is SlackDatasetErrorPrimaryPipeline
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=derived_dataset_context_mock_value,
        ):
            derived_dataset_mgr = self.slack_manager(
                Statuses.DATASET_ERROR,
                **{"messages": {"primary_dataset_uuid": "test_primary_uuid"}},
            )
            assert type(derived_dataset_mgr.message_class) is SlackDatasetErrorDerived

    def test_invalid_classes(self):
        upload_mgr = self.slack_manager(Statuses.UPLOAD_INVALID)
        assert type(upload_mgr.message_class) is SlackUploadInvalid
        dataset_mgr = self.slack_manager(Statuses.DATASET_INVALID)
        assert type(dataset_mgr.message_class) is SlackDatasetInvalid

    def test_new_classes(self):
        self.status_context.stop()
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=dataset_context_mock_value,
        ):
            dataset_mgr = self.slack_manager(Statuses.DATASET_NEW)
            assert type(dataset_mgr.message_class) is SlackDatasetNew
            with self.assertRaises(NotImplementedError):
                dataset_mgr.message_class.format()
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=derived_dataset_context_mock_value,
        ):
            derived_dataset_mgr = self.slack_manager(
                Statuses.DATASET_NEW, **{"messages": {"processing_pipeline": "test_pipeline"}}
            )
            assert type(derived_dataset_mgr.message_class) is SlackDatasetNewDerived

    def test_qa_classes(self):
        self.status_context.stop()
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=dataset_context_mock_value,
        ):
            dataset_mgr = self.slack_manager(Statuses.DATASET_QA)
            assert type(dataset_mgr.message_class) is SlackDatasetQA
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=derived_dataset_context_mock_value,
        ):
            derived_dataset_mgr = self.slack_manager(
                Statuses.DATASET_QA, **{"messages": {"processing_pipeline": "test_pipeline"}}
            )
            assert type(derived_dataset_mgr.message_class) is SlackDatasetQADerived

    def test_reorganized_no_datasets(self):
        self.status_context.stop()
        self.slack_msg_context.stop()
        # add priority key to make sure SlackUploadReorganizedPriority doesn't trigger
        additions = good_upload_context | {
            "priority_project_list": ["PRIORITY"],
        }
        additions.pop("datasets")
        with patch("status_change.status_utils.get_submission_context", return_value=additions):
            no_datasets_upload_reorg_mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED)
        assert type(no_datasets_upload_reorg_mgr.message_class) is SlackUploadReorganizedNoDatasets
        with self.assertRaises(NotImplementedError):
            no_datasets_upload_reorg_mgr.message_class.format()
        self.mock_status_context.reset_mock()
        self.mock_slack_msg_context.reset_mock()

    def test_reorganized_priority(self):
        self.status_context.stop()
        self.slack_msg_context.stop()
        additions = good_upload_context | {
            "datasets": [{"dataset_type": "test_1"}],
            "priority_project_list": ["PRIORITY"],
        }
        with patch("status_change.status_utils.get_submission_context", return_value=additions):
            priority_reorg_dataset_mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED)
        assert type(priority_reorg_dataset_mgr.message_class) is SlackUploadReorganizedPriority
        self.mock_status_context.reset_mock()
        self.mock_slack_msg_context.reset_mock()

    def test_reorganized(self):
        self.status_context.stop()
        self.slack_msg_context.stop()
        additions = good_upload_context | {"datasets": [{"dataset_type": "test_1"}]}
        additions.pop("priority_project_list")
        with patch("status_change.status_utils.get_submission_context", return_value=additions):
            upload_mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED)
        assert type(upload_mgr.message_class) is SlackUploadReorganized
        self.mock_status_context.reset_mock()
        self.mock_slack_msg_context.reset_mock()


class TestFailureCallback(MockParent):
    @patch("traceback.TracebackException.from_exception")
    def test_failure_callback(self, tbfa_mock):
        self.entity_update.stop()

        def _xcom_getter(key):
            return {"uuid": "abc123"}[key]

        class _exception_formatter:
            def __init__(self, excp_str):
                self.excp_str = excp_str

            def format(self):
                return f"This is the formatted version of {self.excp_str}"

        with patch("status_change.status_utils.HttpHook.run") as hhr_mock:
            with patch("status_change.callbacks.base.get_auth_tok", return_value="auth_token"):
                hhr_mock.return_value.json.return_value = {
                    "entity_type": "upload",
                    "status": "new",
                }
                tbfa_mock.side_effect = lambda excp: _exception_formatter(excp)
                dag_run_mock = MagicMock(
                    conf={"dryrun": False},
                    dag_id="test_dag_id",
                    execution_date=date.fromisoformat("2025-06-05"),
                )
                task_instance_mock = MagicMock()
                task_instance_mock.xcom_pull.side_effect = _xcom_getter
                tweaked_ctx = good_upload_context.copy() | {
                    "task_instance": task_instance_mock,
                    "task": MagicMock(task_id="mytaskid"),
                    "crypt_auth_tok": "test_crypt_auth_tok",
                    "dag_run": dag_run_mock,
                    "exception": "FakeTestException",
                }
                fcb = FailureCallback(__name__)
                fcb(tweaked_ctx)
                assert fcb.task
                assert "mytaskid" == fcb.task.task_id
                assert fcb.dag_run
                assert "test_dag_id" == fcb.dag_run.dag_id
                assert date(2025, 6, 5) == fcb.dag_run.execution_date
                assert __name__ == fcb.called_from
                assert (
                    "This is the formatted version of FakeTestException" == fcb.formatted_exception
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


class TestDataIngestBoardManager(MockParent):

    def test_clear_status(self):
        dib = DataIngestBoardManager(Statuses.UPLOAD_VALID, "test_uuid", "test_token")
        assert dib.get_fields() == {"error_message": ""}

    @patch("status_change.data_ingest_board_manager.DataIngestBoardManager.upload_invalid")
    def test_valid_status_from_statuschanger(self, upload_invalid_mock):
        sc = StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="Invalid",
            extra_options={},
        )
        sc.update()
        self.mock_dib_update.assert_called_once()
        upload_invalid_mock.assert_called_once()

    def test_valid_status_return_ext_error(self):
        # invalid status, validation output doesn't have any internal error strings
        dib = DataIngestBoardManager(
            Statuses.UPLOAD_INVALID, "test_uuid", "test_token", messages={"run_id": "test_run_id"}
        )
        assert dib.get_fields() == {
            "error_message": f"Invalid status from run test_run_id",
            "assigned_to_group_name": "test group",
        }

    def test_valid_status_internal_error_strs(self):
        # invalid status but internal error in validation message
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=self.mock_status_context.return_value.copy()
            | {"validation_message": "Internal error--test"},
        ):
            dib = DataIngestBoardManager(
                Statuses.UPLOAD_INVALID,
                "test_uuid",
                "test_token",
                messages={"run_id": "test_run_id"},
            )
            assert dib.get_fields() == {
                "error_message": "Internal error. Log directory: test_path/test_run_id",
                "assigned_to_group_name": "IEC Testing Group",
            }
            dib_w_msg = DataIngestBoardManager(
                Statuses.UPLOAD_INVALID,
                "test_uuid",
                "test_token",
                messages={
                    "error_counts": {"Antibodies/Contributors Errors": "1", "Test errors": "5"},
                    "run_id": "test_run_id",
                },
            )
            assert dib_w_msg.get_fields() == {
                "error_message": "Internal error. Log directory: test_path/test_run_id | Antibodies/Contributors Errors: 1; Test errors: 5",
                "assigned_to_group_name": "IEC Testing Group",
            }

    def test_valid_status_internal_error_status(self):
        # error status, message doesn't matter
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=self.mock_status_context.return_value.copy()
            | {
                "error_message": "Internal error--test",
                "status": "error",
            },
        ):
            dib = DataIngestBoardManager(
                Statuses.UPLOAD_ERROR,
                "test_uuid",
                "test_token",
                messages={"run_id": "test_run_id"},
            )
            assert dib.get_fields() == {
                "error_message": "Internal error. Log directory: test_path/test_run_id",
                "assigned_to_group_name": "IEC Testing Group",
            }
            dib_w_msg = DataIngestBoardManager(
                Statuses.UPLOAD_ERROR,
                "test_uuid",
                "test_token",
                messages={
                    "error_counts": {"Directory errors": "3", "Plugins skipped": "True"},
                    "run_id": "test_run_id",
                },
            )
            assert dib_w_msg.get_fields() == {
                "error_message": f"Internal error. Log directory: test_path/test_run_id | Directory errors: 3; Plugins skipped: True",
                "assigned_to_group_name": "IEC Testing Group",
            }
        with patch(
            "status_change.status_utils.get_submission_context",
            return_value=self.mock_status_context.return_value.copy()
            | {
                "error_message": "",
                "status": "error",
            },
        ):
            dib = DataIngestBoardManager(
                Statuses.UPLOAD_ERROR,
                "test_uuid",
                "test_token",
                messages={"run_id": "test_run_id"},
            )
            assert dib.get_fields() == {
                "error_message": "Internal error. Log directory: test_path/test_run_id",
                "assigned_to_group_name": "IEC Testing Group",
            }

    def test_status_invalid_for_manager(self):
        dib = DataIngestBoardManager(Statuses.DATASET_HOLD, "test_uuid", "test_token")
        assert dib.get_fields() == None
        assert dib.is_valid_for_status == False

    def test_get_msg_ext_error(self):
        dib = DataIngestBoardManager(
            Statuses.UPLOAD_INVALID,
            "test_uuid",
            "test_token",
            messages={"error_counts": {"Antibodies/Contributors Errors": "1"}},
        )
        assert dib.get_fields() == {
            "error_message": "Antibodies/Contributors Errors: 1",
            "assigned_to_group_name": "test group",
        }

    @patch("status_change.data_ingest_board_manager.put_request_to_entity_api")
    def test_clear_derived(self, mock_update):
        self.status_context.stop()
        self.dib_update.stop()
        with patch(
            "status_change.data_ingest_board_manager.get_primary_dataset", return_value="test_uuid"
        ):
            with patch(
                "status_change.status_utils.get_submission_context",
                return_value=derived_dataset_context_mock_value,
            ):
                with patch(
                    "status_change.data_ingest_board_manager.get_submission_context",
                    return_value=dataset_context_mock_value_with_error,
                ):
                    DataIngestBoardManager(
                        Statuses.DATASET_QA,
                        "test_derived",
                        "test_token",
                        messages={"primary_dataset_uuid": "test_primary_uuid"},
                    ).update()
                    mock_update.assert_has_calls(
                        [
                            call("test_derived", "test_token", {"error_message": ""}),
                            call("test_uuid", "test_token", {"error_message": ""}),
                        ],
                        any_order=True,
                    )

    @patch("status_change.data_ingest_board_manager.put_request_to_entity_api")
    def test_dont_clear_derived(self, mock_update):
        self.status_context.stop()
        self.dib_update.stop()
        with patch(
            "status_change.data_ingest_board_manager.get_primary_dataset", return_value="test_uuid"
        ):
            with patch(
                "status_change.status_utils.get_submission_context",
                return_value=dataset_context_mock_value,
            ):
                with patch(
                    "status_change.data_ingest_board_manager.get_submission_context",
                    return_value=dataset_context_mock_value,
                ):
                    DataIngestBoardManager(
                        Statuses.DATASET_QA,
                        "test_derived",
                        "test_token",
                        messages={"primary_dataset_uuid": "test_primary_uuid"},
                    ).update()
                    mock_update.assert_called_once()

    @patch("status_change.data_ingest_board_manager.put_request_to_entity_api")
    def test_dataset_error_derived(self, mock_update):
        self.status_context.stop()
        self.dib_update.stop()
        with patch(
            "status_change.data_ingest_board_manager.get_primary_dataset", return_value="test_uuid"
        ):
            with patch(
                "status_change.status_utils.get_submission_context",
                return_value=derived_dataset_context_mock_value,
            ):
                with patch(
                    "status_change.data_ingest_board_manager.get_submission_context",
                    return_value=dataset_context_mock_value_with_error,
                ):
                    DataIngestBoardManager(
                        Statuses.DATASET_ERROR,
                        "test_derived",
                        "test_token",
                        messages={"primary_dataset_uuid": "test_primary_uuid"},
                    ).update()
                    mock_update.assert_called_once()
                    mock_update.assert_called_with(
                        "test_uuid",
                        "test_token",
                        {
                            "error_message": "Derived dataset test_derived is in Error state.",
                            "assigned_to_group_name": "",
                        },
                    )

    def test_dataset_error_primary_not_found(self):
        self.status_context.stop()
        self.dib_update.stop()
        with patch("status_change.data_ingest_board_manager.get_primary_dataset", return_value=""):
            with patch(
                "status_change.status_utils.get_submission_context",
                return_value=derived_dataset_context_mock_value,
            ):
                with patch(
                    "status_change.data_ingest_board_manager.get_submission_context",
                    return_value=dataset_context_mock_value,
                ):
                    with self.assertRaises(EntityUpdateException):
                        DataIngestBoardManager(
                            Statuses.DATASET_ERROR,
                            "test_derived",
                            "test_token",
                            messages={"primary_dataset_uuid": "test_primary_uuid"},
                        ).update()


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
            (
                {"error_message": "Internal error: test error", "status": "invalid"},
                False,
            ),  # ignore existing error messages if status is not error, they are likely left over from previous error status
            (
                {"validation_message": "Internal error: test error", "status": "invalid"},
                True,
            ),  # catch internal error strings in validation_message if status is not error
            ({"validation_message": "Test error", "status": "invalid"}, False),  #
            ({"error_message": "Directory errors: 5", "status": "invalid"}, False),
        ]

        for entity_data, is_error in entity_data_dicts:
            assert is_internal_error(entity_data) == is_error, f"{entity_data} is not {is_error}"

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_env(self, conn_mock):
        for proj in ["hubmap", "sennet"]:
            with patch("utils.ENDPOINTS", endpoints[proj]):
                for env, host_url in {
                    "prod": f"https://entity.api.{proj}consortium.org",
                    "dev": f"https://entity-api.dev.{proj}consortium.org",
                    None: "https://badurl",
                }.items():
                    conn_mock.return_value = Connection(host=host_url)
                    assert get_env() == env

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_entity_ingest_url(self, hhr_mock):
        for proj in ["hubmap", "sennet"]:
            for entity_api, data in {
                f"https://entity-api.dev.{proj}consortium.org": {
                    f"https://ingest.dev.{proj}consortium.org": {
                        "entity_type": "upload",
                        "uuid": f"test_{proj}_uuid",
                    }
                },
                f"https://entity.api.{proj}consortium.org": {
                    f"https://ingest.{proj}consortium.org": {
                        "entity_type": "dataset",
                        "uuid": f"test_{proj}_uuid",
                    }
                },
            }.items():
                for url_prefix, entity_data in data.items():
                    hhr_mock.return_value = Connection(host=entity_api)
                    with patch("utils.ENDPOINTS", endpoints[proj]):
                        url = get_entity_ingest_url(entity_data)
                        expected_url = (
                            f"{url_prefix}/{entity_data['entity_type']}/{entity_data['uuid']}"
                        )
                        print(f"assert {url} == {expected_url}")
                        assert url == expected_url

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_data_ingest_board_query_url(self, hhr_mock):
        for proj in ["hubmap", "sennet"]:
            with patch("utils.ENDPOINTS", endpoints[proj]):
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
            "sn_public_uuid_dev": {
                "sennet": {
                    "abs_path": "/codcc-dev/data/public/sn_public_uuid_dev",
                    "dest_path": "https://app.globus.org/file-manager?origin_id=96b2b9e5-6915-4dbc-9ab5-173ad628902e&origin_path=sn_public_uuid_dev",
                }
            },
            "hm_public_uuid_dev": {
                "hubmap": {
                    "abs_path": "/hive/hubmap-dev/data/public/hm_public_uuid_dev",
                    "dest_path": "https://app.globus.org/file-manager?origin_id=af603d86-eab9-4eec-bb1d-9d26556741bb&origin_path=hm_public_uuid_dev",
                }
            },
            "sn_protected_uuid_prod": {
                "sennet": {
                    "abs_path": "/codcc-prod/data/protected/component/sn_protected_uuid_prod",
                    "dest_path": "https://app.globus.org/file-manager?origin_id=45617036-f2cc-4320-8108-edf599290158&origin_path=%2Fprotected%2Fcomponent%2Fsn_protected_uuid_prod%2F",
                }
            },
            "hm_protected_uuid_prod": {
                "hubmap": {
                    "abs_path": "/hive/hubmap/data/protected/component/hm_protected_uuid_prod",
                    "dest_path": "https://app.globus.org/file-manager?origin_id=24c2ee95-146d-4513-a1b3-ac0bfdb7856f&origin_path=%2Fprotected%2Fcomponent%2Fhm_protected_uuid_prod%2F",
                }
            },
        }
        for uuid, data in test_data.items():
            for proj, paths in data.items():
                with patch("utils.ENDPOINTS", endpoints[proj]):
                    abs_path_mock.return_value = paths["abs_path"]
                    conn_mock.return_value = Connection(
                        host=f"https://entity.api.{proj}consortium.org"
                    )
                    print(get_globus_url(uuid, "test_token"))
                    assert get_globus_url(uuid, "test_token") == paths["dest_path"]


class MockedEmailManager(EmailManager):
    # This functionality is currently turned off for QA on prod;
    # sending only to internal recipients. This mocks the correct
    # functionality. TODO: remove after turning on ext emails.
    def get_recipients(self):
        conf_dict = airflow_conf.as_dict()
        # Allows for setting defaults at the config level that override class defaults, e.g. for testing
        if int_recipients := conf_dict.get("email_notifications", {}).get("int_recipients"):
            cleaned_int_recipients = [str(address) for address in [int_recipients]]
            self.int_recipients = cleaned_int_recipients
        if main_recipient := conf_dict.get("email_notifications", {}).get("main"):
            self.primary_contact = main_recipient

        if self.is_internal_error:
            self.main_recipients = ", ".join(self.int_recipients)
        else:
            self.main_recipients = self.primary_contact
            self.cc = ", ".join(self.int_recipients)


class TestEmailManager(MockParent):
    int_recipients = "int@test.com"

    def email_manager(
        self,
        status: Statuses,
        messages: Optional[dict] = None,
        context: dict = good_upload_context,
        mock: bool = False,
    ):
        with patch("status_change.status_utils.get_submission_context") as context_mock:
            manager_class = MockedEmailManager if mock else EmailManager
            context_mock.return_value = context | {"status": Statuses.valid_str(status)}
            manager = manager_class(
                status,
                "test_uuid",
                "test_token",
                messages=messages or {} | {"run_id": "test_run_id"},
            )
            manager.int_recipients = [self.int_recipients]
            return manager

    def test_is_valid_for_status(self):
        assert self.email_manager(Statuses.UPLOAD_ERROR).is_valid_for_status == True

    def test_is_not_valid_for_status(self):
        assert self.email_manager(Statuses.UPLOAD_NEW).is_valid_for_status == False

    def test_get_contacts_error(self):
        manager = self.email_manager(
            Statuses.DATASET_ERROR, context=dataset_context_mock_value, mock=True
        )
        manager.get_recipients()
        assert manager.main_recipients == self.int_recipients
        assert manager.cc == ""

    def test_get_contacts_invalid(self):
        manager = self.email_manager(Statuses.UPLOAD_INVALID, mock=True)
        manager.get_recipients()
        assert manager.main_recipients == good_upload_context.get("created_by_user_email")
        assert manager.cc == self.int_recipients

    def test_get_contacts_good(self):
        manager = self.email_manager(
            Statuses.DATASET_QA, context=dataset_context_mock_value, mock=True
        )
        manager.get_recipients()
        assert manager.main_recipients == dataset_context_mock_value.get("created_by_user_email")
        assert manager.cc == self.int_recipients

    def test_get_contacts_good_with_config_override(self):
        with patch(
            "utils.airflow_conf.as_dict",
            return_value={
                "email_notifications": {"int_recipients": "int@test.com", "main": "main@test.com"},
                "connections": {"WORKFLOW_SCRATCH": "test_path"},
            },
        ):
            manager = self.email_manager(
                Statuses.DATASET_QA, context=dataset_context_mock_value, mock=True
            )
            manager.get_recipients()
            assert manager.main_recipients == "main@test.com"
            assert manager.cc == "int@test.com"

    def test_get_content_error(self):
        manager = self.email_manager(
            Statuses.UPLOAD_ERROR,
            context=good_upload_context | {"error_message": "An error has occurred"},
        )
        expected_subj = "Internal error for Upload test_hm_id"
        expected_msg = "HuBMAP ID: test_hm_id<br>UUID: test_uuid<br>Entity type: Upload<br>Status: Error<br>Group: test group<br>Primary contact: test@user.com<br>Ingest page: https://ingest.hubmapconsortium.org/upload/test_uuid<br>Log file: test_path/test_run_id<br>"
        # print(f"Expected subject: {expected_subj}")
        # print(f"Actual subj: {manager.subj}")
        # print(f"Expected msg: {expected_msg}")
        # print(f"Actual msg: {manager.msg}")
        assert manager.subj == expected_subj
        assert manager.msg == expected_msg

    def test_get_content_invalid(self):
        manager = self.email_manager(
            Statuses.UPLOAD_INVALID,
            context=good_upload_context,
            messages={
                "error_counts": {"Directory errors": "3", "Plugins skipped": "True"},
                "error_dict": {
                    "Directory Errors": {
                        "/hive/hubmap-dev/data/protected/IEC Testing Group/dc3f82820dca46f2bd86d8a8641afd25/RI_LA1D_AB-PAS (as histology-v2.3)": {
                            "Required but missing": ["extras\\/microscope_hardware\\.json"]
                        },
                        "/hive/hubmap-dev/data/protected/IEC Testing Group/dc3f82820dca46f2bd86d8a8641afd25/RI_LA2D_AB-PAS (as histology-v2.3)": {
                            "Required but missing": ["extras\\/microscope_hardware\\.json"]
                        },
                    },
                    "Fatal Errors": "Skipping plugin validation due to errors in upload metadata or dir structure.",
                },
            },
        )
        expected_subj = f"HuBMAP Upload test_hm_id is invalid"
        expected_msg = 'HuBMAP Upload <a href="https://ingest.hubmapconsortium.org/upload/test_uuid">test_hm_id</a> has failed validation.<br><br><b>Validation details</b><br>The validation process starts by checking metadata TSVs and directory structures. If those checks pass, then certain individual file types (such as FASTQ and OME.TIFF files) are validated.<br><br><b>What to do next</b><br>If you have questions about your upload, please schedule an appointment with Data Curator Brendan Honick (https://calendly.com/bhonick-psc/) or email ingest@hubmapconsortium.org. Do not respond to this email; this inbox is not monitored.<br><br>The error log is included below if you would like to make updates to your submission independently. It is not required for you to correct any errors before contacting our Data Curation Team. Please email ingest@hubmapconsortium.org if you believe you have repaired all validation errors so that we can re-validate your submission.<br><br>If your submission has "Spreadsheet Validator Errors," please use the <a href="https://metadatavalidator.metadatacenter.org/">Metadata Spreadsheet Validator</a> tool to correct them.<br><br><b>Validation error log</b><br><ul><li>Directory Errors:</li><ul><li>/hive/hubmap-dev/data/protected/IEC Testing Group/dc3f82820dca46f2bd86d8a8641afd25/RI_LA1D_AB-PAS (as histology-v2.3):</li><ul><li>Required but missing:</li><ul><li>extras\\/microscope_hardware\\.json</li></ul></ul><li>/hive/hubmap-dev/data/protected/IEC Testing Group/dc3f82820dca46f2bd86d8a8641afd25/RI_LA2D_AB-PAS (as histology-v2.3):</li><ul><li>Required but missing:</li><ul><li>extras\\/microscope_hardware\\.json</li></ul></ul></ul><li>Fatal Errors:</li><ul><li>Skipping plugin validation due to errors in upload metadata or dir structure.</li></ul></ul>'
        # print(f"Expected subject: {expected_subj}")
        # print(f"Actual subj: {manager.subj}")
        # print(f"Expected msg: {expected_msg}")
        # print(f"Actual msg: {manager.msg}")
        assert manager.subj == expected_subj
        assert manager.msg == expected_msg

    def test_get_content_good(self):
        expected_subj = f"Upload test_hm_id has successfully reached status Valid!"
        expected_msg = "View ingest record: https://ingest.hubmapconsortium.org/upload/test_uuid<br><br>This email address is not monitored. Please email ingest@hubmapconsortium.org with any questions about your data submission.<br>"
        manager = self.email_manager(Statuses.UPLOAD_VALID)
        # print(f"Expected subject: {expected_subj}")
        # print(f"Actual subj: {manager.subj}")
        # print(f"Expected msg: {expected_msg}")
        # print(f"Actual msg: {manager.msg}")
        assert manager.subj == expected_subj
        assert manager.msg == expected_msg

    def test_send(self):
        manager = self.email_manager(EmailManager.good_statuses[0])
        manager.update()
        assert self.mock_email_send.called_once()

    def test_send_error_formatting(self):
        manager = self.email_manager(
            Statuses.UPLOAD_INVALID,
            context=good_upload_context
            | {"error_message": "Directory errors: 3; Plugins skipped: True"},
        )
        manager.update()
        error_msg = """
            HuBMAP ID: test_hm_id<br>
            Group: test group<br>
            Ingest page: https://ingest.hubmapconsortium.org/upload/test_uuid<br>
            <br>
            Upload is invalid:<br>
            - Directory errors: 3<br>
            - Plugins skipped: True<br>
            <br>
            <br>
            This email address is not monitored. Please email ingest@hubmapconsortium.org with any questions about your data submission.
            """
        assert self.mock_email_send.called_once_with(
            manager.main_recipients, "Upload test_upload is invalid", error_msg, manager.cc
        )

    def test_reorg_has_child_datasets(self):
        # reorg has datasets, return True
        self.assertTrue(
            self.email_manager(
                Statuses.UPLOAD_REORGANIZED, context=good_upload_context
            ).reorg_status_with_child_datasets()
        )
        # reorg has no datasets, return False
        self.assertFalse(
            self.email_manager(
                Statuses.UPLOAD_REORGANIZED,
                context=good_upload_context | {"datasets": []},
            ).reorg_status_with_child_datasets()
        )
        # non-reorg has datasets, return False
        self.assertFalse(
            self.email_manager(
                Statuses.UPLOAD_VALID, context=good_upload_context
            ).reorg_status_with_child_datasets()
        )
        # non-reorg has no datasets, return False
        self.assertFalse(
            self.email_manager(
                Statuses.UPLOAD_VALID,
                context=good_upload_context | {"datasets": []},
            ).reorg_status_with_child_datasets()
        )

    def test_reorg_format(self):
        mgr = self.email_manager(Statuses.UPLOAD_REORGANIZED, context=good_upload_context)
        assert (
            mgr.msg
            == "View ingest record: https://ingest.hubmapconsortium.org/upload/test_uuid<br><br>Datasets:<br><ul><li><a href='https://ingest.hubmapconsortium.org/dataset/test_dataset_uuid'>test_dataset_hm_id</a>: submitted</li><li><a href='https://ingest.hubmapconsortium.org/dataset/test_dataset_uuid2'>test_dataset_hm_id2</a>: submitted</li></ul><br>This email address is not monitored. Please email ingest@hubmapconsortium.org with any questions about your data submission.<br>"
        )

    def test_ext_error_format(self):
        mgr = self.email_manager(
            Statuses.UPLOAD_INVALID,
            context=good_upload_context,
            messages={
                "error_counts": "Antibodies/Contributors Errors: 1",
                "error_dict": validation_error_dict,
            }
            | {"status": "Invalid"},
        )
        assert mgr.msg == ext_error

    def test_error_formatter(self):
        manager = self.email_manager(Statuses.UPLOAD_INVALID, mock=True)
        template = InvalidStatusEmail(manager)
        just_lines = template.recursive_format_dict(validation_error_dict)
        html_format = template.recursive_format_dict(validation_error_dict, html=True)
        assert just_lines == [
            "Directory Errors:",
            "examples/dataset-examples/bad-scatacseq-data/upload/dataset-1 (as scatacseq-v0.0):",
            "Not allowed:",
            "not-the-file-you-are-looking-for.txt",
            "unexpected-directory/place-holder.txt",
            "Required but missing:",
            "[^/]+\\.fastq\\.gz",
            "Antibodies/Contributors Errors:",
            "examples/dataset-examples/bad-scatacseq-data/upload/scatacseq-metadata.tsv:",
            'On row(s) 2, column "contributors_path", error opening or reading value ".". Expected a TSV, but found a directory: examples/dataset-examples/bad-scatacseq-data/upload.',
            "Local Validation Errors:",
            "examples/dataset-examples/bad-scatacseq-data/upload/scatacseq-metadata.tsv (as scatacseq-v0):",
            'On row 2, column "sc_isolation_protocols_io_doi", value "" fails because it must be filled out.',
            'On row 2, column "library_construction_protocols_io_doi", value "" fails because it must be filled out.',
            'On row 2, column "protocols_io_doi", value "10.17504/fake" fails because it is an invalid DOI.',
        ]
        assert html_format == validation_report_html_list
