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

    def test_dollar_case(self):
        from template_utils import TemplateBuilder
        files_l = [
            {"rel_path": "ometiff-pyramids/pipeline_output/mask/reg001_mask.ome.tif",
             "type": "unknown", "size": 9011601515,
             "description": "OME-TIFF$ $ pyrami$d file",
             "edam_term": "EDAM_1.24.format_3727",
             "is_qa_qc": False, "is_data_product": False
             },
            {"rel_path": "ometiff-pyramids/pipeline_output/expr/reg001_expr.ome.tif",
             "type": "unknown", "size": 30416407302,
             "description": "OME-TIFF pyramid file", 
             "edam_term": "EDAM_1.24.format_3727",
             "is_qa_qc": False, "is_data_product": False
             }
        ]
        templated_files_l = TemplateBuilder(files_l).apply()
        expanded_files_l = TemplateBuilder.expand(templated_files_l)
        self.assertEqual(files_l, expanded_files_l)
