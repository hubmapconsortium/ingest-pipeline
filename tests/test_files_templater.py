import json
import unittest
import zipfile

class TestFilesTemplater(unittest.TestCase):
    def test_templater(self):
        from template_utils import TemplateBuilder
        with zipfile.ZipFile("tests/sample_files_json.zip", "r") as zip_ref:
            data = zip_ref.open("sample.json")
            files_md = json.load(data)
        files_l = files_md["files"]
        templated_files_l = TemplateBuilder(files_l).apply()
        expanded_files_l = TemplateBuilder.expand(templated_files_l)
        self.assertEqual(files_l, expanded_files_l)
    