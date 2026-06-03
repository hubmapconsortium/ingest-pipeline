import unittest

from utils import assert_qa_or_better


class TestUtils(unittest.TestCase):
    def test_assert_qa_or_better(self):
        assert_qa_or_better("QA")
        assert_qa_or_better("Approval")
        assert_qa_or_better("Published")
        assert_qa_or_better("Invalid", ["Invalid"])
        with self.assertRaises(AssertionError):
            assert_qa_or_better("Invalid")
            assert_qa_or_better("Invalid", ["New"])
