import unittest
from unittest.mock import patch

from status_manager import StatusChanger, StatusChangerException, Statuses
from utils import pythonop_set_dataset_state


class TestStatusChanger(unittest.TestCase):
    upload_valid = StatusChanger(
        "upload_valid_uuid",
        "upload_valid_token",
        "Valid",
        {
            "extra_fields": {},
            "extra_options": {},
        },
        entity_type="Upload",
    )

    def test_unrecognized_status(self):
        with self.assertRaises(StatusChangerException):
            StatusChanger(
                "invalid_status_uuid",
                "invalid_status_token",
                "invalid_status_string",
                {
                    "extra_fields": {},
                    "extra_options": {},
                },
                entity_type="Upload",
            )

    def test_recognized_status(self):
        data = self.upload_valid.format_status_data()
        self.assertEqual(data["status"], self.upload_valid.status)

    def test_extra_fields(self):
        with_extra_field = StatusChanger(
            "extra_field_uuid",
            "extra_field_token",
            Statuses.UPLOAD_PROCESSING,
            {
                "extra_fields": {"test_extra_field": True},
                "extra_options": {},
            },
        )
        data = with_extra_field.format_status_data()
        self.assertIn("test_extra_field", data)
        self.assertEqual(data["test_extra_field"], True)

    @patch("status_manager.HttpHook.run")
    def test_extra_options(self, hhr_mock):
        with_extra_option = StatusChanger(
            "extra_options_uuid",
            "extra_options_token",
            Statuses.UPLOAD_VALID,
            {"extra_fields": {}, "extra_options": {"check_response": False}},
            verbose=False,
        )
        with_extra_option.set_entity_api_status()
        self.assertIn({"check_response": False}, hhr_mock.call_args.args)
        without_extra_option = StatusChanger(
            "extra_options_uuid",
            "extra_options_token",
            Statuses.UPLOAD_VALID,
            {"extra_fields": {}, "extra_options": {}},
            verbose=False,
        )
        without_extra_option.set_entity_api_status()
        self.assertIn({"check_response": True}, hhr_mock.call_args.args)

    @patch("status_manager.HttpHook.run")
    def test_extra_options_and_fields(self, hhr_mock):
        with_extra_option_and_field = StatusChanger(
            "extra_options_uuid",
            "extra_options_token",
            Statuses.UPLOAD_VALID,
            {
                "extra_fields": {"test_extra_field": True},
                "extra_options": {"check_response": False},
            },
            verbose=False,
        )
        with_extra_option_and_field.set_entity_api_status()
        self.assertIn({"check_response": False}, hhr_mock.call_args.args)
        self.assertIn('{"status": "Valid", "test_extra_field": true}', hhr_mock.call_args.args)

    @patch("status_manager.HttpHook.run")
    def test_valid_status_in_request(self, hhr_mock):
        self.upload_valid.set_entity_api_status()
        self.assertIn('{"status": "Valid"}', hhr_mock.call_args.args)

    def test_http_conn_id(self):
        with_http_conn_id = StatusChanger(
            "http_conn_uuid",
            "http_conn_token",
            Statuses.DATASET_NEW,
            {
                "extra_fields": {},
                "extra_options": {},
            },
            http_conn_id="test_conn_id",
        )
        assert with_http_conn_id.http_conn_id == "test_conn_id"

    @patch("status_manager.HttpHook.run")
    @patch("status_manager.StatusChanger.send_email")
    def test_status_map(self, test_send_email, hhr_mock):
        self.assertFalse(test_send_email.called)
        self.upload_valid.status_map = {Statuses.UPLOAD_VALID: [self.upload_valid.send_email]}
        self.upload_valid.on_status_change()
        self.assertTrue(test_send_email.called)

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
            Statuses.DATASET_PROCESSING,
            {
                "extra_fields": {"pipeline_message": message},
                "extra_options": {},
            },
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
            Statuses.DATASET_QA,
            {
                "extra_fields": {"pipeline_message": message},
                "extra_options": {},
            },
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
