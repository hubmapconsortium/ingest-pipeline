import unittest
from functools import cached_property
from unittest.mock import patch

from status_manager import EntityUpdateException, EntityUpdater, StatusChanger, Statuses
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
    @patch("status_manager.get_submission_context")
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

    @patch("status_manager.EntityUpdater._validate_fields_to_change")
    @patch("status_manager.HttpHook.run")
    def test_update(self, hhr_mock, validate_mock):
        hhr_mock.assert_not_called()
        validate_mock.assert_not_called()
        self.upload_entity_valid.update()
        validate_mock.assert_called_once()
        hhr_mock.assert_called_once()

    @patch("status_manager.StatusChanger")
    @patch("status_manager.get_submission_context")
    def test_pass_to_statuschanger(self, context_mock, statuschanger_mock):
        context_mock.return_value = self.good_context
        with_status = EntityUpdater(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"status": "valid", "validation_message": self.validation_msg},
            fields_to_append_to={"ingest_task": self.ingest_task},
        )
        with patch("status_manager.HttpHook.run"):
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
        with patch("status_manager.get_submission_context"):
            return StatusChanger(
                "upload_valid_uuid",
                "upload_valid_token",
                status="Valid",
                extra_options={},
                entity_type="Upload",
            )

    def test_unrecognized_status(self):
        with patch("status_manager.get_submission_context"):
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
        with patch("status_manager.get_submission_context"):
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
        with patch("status_manager.get_submission_context"):
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

    @patch("status_manager.get_submission_context")
    @patch("status_manager.HttpHook.run")
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

    @patch("status_manager.get_submission_context")
    @patch("status_manager.HttpHook.run")
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

    @patch("status_manager.HttpHook.run")
    def test_valid_status_in_request(self, hhr_mock):
        self.upload_valid._validate_fields_to_change()
        self.upload_valid._set_entity_api_data()
        self.assertIn('{"status": "valid"}', hhr_mock.call_args.args)

    def test_http_conn_id(self):
        with patch("status_manager.HttpHook.run"):
            with_http_conn_id = StatusChanger(
                "http_conn_uuid",
                "http_conn_token",
                status=Statuses.DATASET_NEW,
                http_conn_id="test_conn_id",
            )
            assert with_http_conn_id.http_conn_id == "test_conn_id"

    @patch("status_manager.StatusChanger.send_email")
    def test_update_from_status_map(self, send_email_mock):
        self.assertFalse(send_email_mock.called)
        self.assertEqual(self.upload_valid.status, Statuses.UPLOAD_VALID)
        self.upload_valid.status_map = {Statuses.UPLOAD_VALID: [self.upload_valid.send_email]}
        with patch("status_manager.HttpHook.run"):
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


if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestEntityUpdater)
    suite.debug()
