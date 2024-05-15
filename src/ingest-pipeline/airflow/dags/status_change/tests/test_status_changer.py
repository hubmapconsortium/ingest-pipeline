import unittest
from functools import cached_property
from unittest.mock import patch

from status_manager import StatusChanger, StatusChangerException, Statuses
from utils import pythonop_set_dataset_state


class TestStatusChanger(unittest.TestCase):
    @cached_property
    @patch("status_manager.HttpHook.run")
    def upload_valid(self, hhr_mock):
        return StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="Valid",
            extra_options={},
            entity_type="Upload",
        )

    @patch("status_manager.HttpHook.run")
    def test_unrecognized_status(self, hhr_mock):
        with self.assertRaises(StatusChangerException):
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

    @patch("status_manager.HttpHook.run")
    def test_extra_fields(self, hhr_mock):
        with_extra_field = StatusChanger(
            "extra_field_uuid",
            "extra_field_token",
            status=Statuses.UPLOAD_PROCESSING,
            fields_to_overwrite={"test_extra_field": True},
        )
        data = with_extra_field.fields_to_change
        self.assertIn("test_extra_field", data)
        self.assertEqual(data["test_extra_field"], True)

    @patch("status_manager.HttpHook.run")
    def test_extra_options(self, hhr_mock):
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

    @patch("status_manager.HttpHook.run")
    def test_extra_options_and_fields(self, hhr_mock):
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
        self.assertIn('{"status": "valid", "test_extra_field": true}', hhr_mock.call_args.args)

    @patch("status_manager.HttpHook.run")
    def test_valid_status_in_request(self, hhr_mock):
        self.upload_valid._validate_fields_to_change()
        self.upload_valid._set_entity_api_data()
        self.assertIn('{"status": "valid"}', hhr_mock.call_args.args)

    @patch("status_manager.HttpHook.run")
    def test_http_conn_id(self, hhr_mock):
        with_http_conn_id = StatusChanger(
            "http_conn_uuid",
            "http_conn_token",
            status=Statuses.DATASET_NEW,
            http_conn_id="test_conn_id",
        )
        assert with_http_conn_id.http_conn_id == "test_conn_id"

    @patch("status_manager.HttpHook.run")
    @patch("status_manager.StatusChanger.send_email")
    def test_status_map(self, send_email_mock, hhr_mock):
        self.assertFalse(send_email_mock.called)
        self.assertEqual(self.upload_valid.status, Statuses.UPLOAD_VALID)
        self.upload_valid.status_map = {Statuses.UPLOAD_VALID: [self.upload_valid.send_email]}
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
        pythonop_set_dataset_state(
            crypt_auth_tok=token,
            dataset_uuid_callable=self.my_callable,
            uuid=uuid,
            message=message,
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
        )
        sc_mock.assert_called_with(
            uuid,
            token,
            status="QA",
            fields_to_overwrite={"pipeline_message": message},
            http_conn_id="entity_api_connection",
        )

    @patch("status_manager.HttpHook.run")
    @patch("utils.get_auth_tok")
    def test_pythonop_set_dataset_state_invalid(self, gat_mock, hhr_mock):
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
