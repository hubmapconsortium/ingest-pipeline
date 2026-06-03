import unittest

from utils import assert_qa_or_better, check_status_is_qa_or_better


class TestUtils(unittest.TestCase):
    def test_assert_qa_or_better(self):
        assert_qa_or_better("QA")
        assert_qa_or_better("Approval")
        assert_qa_or_better("Published")
        assert_qa_or_better("Invalid", ["Invalid"])
        with self.assertRaises(AssertionError):
            assert_qa_or_better("Invalid")
            assert_qa_or_better("Invalid", ["New"])

    def test_check_status_is_qa_or_better(self):
        assert check_status_is_qa_or_better("QA") == "QA"
        assert check_status_is_qa_or_better("Approval") == "Approval"
        assert check_status_is_qa_or_better("Published") == "Published"
        assert check_status_is_qa_or_better("Invalid", ["Invalid"]) == "Invalid"
        assert check_status_is_qa_or_better("Invalid") == None
        assert check_status_is_qa_or_better("Invalid", ["New"]) == None
