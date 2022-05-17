import sys
import os
from importlib import import_module

sys.path.append(os.path.join(os.path.dirname(__file__),
                             'ingest-validation-tools', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__),
                             'ingest-validation-tests', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__),
                             'py-hubmapbags'))

ingest_validation_tools_upload = import_module('ingest_validation_tools.upload')
ingest_validation_tools_error_report = import_module('ingest_validation_tools.error_report')
ingest_validation_tools_validation_utils = import_module('ingest_validation_tools.validation_utils')
ingest_validation_tests = import_module('ingest_validation_tests')
hubmapbags = import_module('hubmapbags')
hubmapbags_magic = import_module('hubmapbags.magic')
hubmapbags_apis = import_module('hubmapbags.apis')
hubmapbags_uuids = import_module('hubmapbags.uuids')
hubmapbags_utilities = import_module('hubmapbags.utilities')
hubmapbags_biosample = import_module('hubmapbags.biosample')
hubmapbags_collection = import_module('hubmapbags.collection')
hubmapbags_file = import_module('hubmapbags.file')
hubmapbags_projects = import_module('hubmapbags.projects')
hubmapbags_id_namespace = import_module('hubmapbags.id_namespace')
hubmapbags_primary_dcc_contact = import_module('hubmapbags.primary_dcc_contact')
hubmapbags_magic = import_module('hubmapbags.magic')
hubmapbags_file_in_collection = import_module('hubmapbags.file_in_collection')
hubmapbags_biosample_in_collection = import_module('hubmapbags.biosample_in_collection')
hubmapbags_subject = import_module('hubmapbags.subject')
hubmapbags_ncbi_taxonomy = import_module('hubmapbags.ncbi_taxonomy')
hubmapbags_subject = import_module('hubmapbags.subject')
hubmapbags_subject_in_collection = import_module('hubmapbags.subject_in_collection')
hubmapbags_biosample_from_subject = import_module('hubmapbags.biosample_from_subject')
hubmapbags_file_describes_biosample = import_module('hubmapbags.file_describes_biosample')
hubmapbags_file_describes_subject = import_module('hubmapbags.file_describes_subject')
hubmapbags_collection_defined_by_project = import_module('hubmapbags.collection_defined_by_project')
hubmapbags_project_in_project = import_module('hubmapbags.project_in_project')
hubmapbags_file_describes_collection = import_module('hubmapbags.file_describes_collection')
hubmapbags_anatomy = import_module('hubmapbags.anatomy')
hubmapbags_assay_type = import_module('hubmapbags.assay_type')
hubmapbags_biosample_disease = import_module('hubmapbags.biosample_disease')
hubmapbags_biosample_gene = import_module('hubmapbags.biosample_gene')
hubmapbags_biosample_substance = import_module('hubmapbags.biosample_substance')
hubmapbags_collection_anatomy = import_module('hubmapbags.collection_anatomy')
hubmapbags_collection_compound = import_module('hubmapbags.collection_compound')
hubmapbags_collection_disease = import_module('hubmapbags.collection_disease')
hubmapbags_collection_gene = import_module('hubmapbags.collection_gene')
hubmapbags_collection_phenotype = import_module('hubmapbags.collection_phenotype')
hubmapbags_collection_protein = import_module('hubmapbags.collection_protein')
hubmapbags_collection_substance = import_module('hubmapbags.collection_substance')
hubmapbags_collection_taxonomy = import_module('hubmapbags.collection_taxonomy')
hubmapbags_collection_in_collection = import_module('hubmapbags.collection_in_collection')
hubmapbags_file_format = import_module('hubmapbags.file_format')

__all__ = ["ingest_validation_tools_validation_utils",
           "ingest_validation_tools_upload",
           "ingest_validation_tools_error_report",
           "ingest_validation_tests",
           "hubmapbags",
           "hubmapbags_magic",
           "hubmapbags_apis",
           "hubmapbags_uuids",
           "hubmapbags_utilities",
           "hubmapbags_biosample",
           "hubmapbags_collection",
           "hubmapbags_file",
           "hubmapbags_projects",
           "hubmapbags_id_namespace",
           "hubmapbags_primary_dcc_contact",
           "hubmapbags_magic",
           "hubmapbags_file_in_collection",
           "hubmapbags_biosample_in_collection",
           "hubmapbags_subject",
           "hubmapbags_ncbi_taxonomy",
           "hubmapbags_subject",
           "hubmapbags_subject_in_collection",
           "hubmapbags_biosample_from_subject",
           "hubmapbags_file_describes_biosample",
           "hubmapbags_file_describes_subject",
           "hubmapbags_collection_defined_by_project",
           "hubmapbags_project_in_project",
           "hubmapbags_file_describes_collection",
           "hubmapbags_anatomy",
           "hubmapbags_assay_type",
           "hubmapbags_biosample_disease",
           "hubmapbags_biosample_gene",
           "hubmapbags_biosample_substance",
           "hubmapbags_collection_anatomy",
           "hubmapbags_collection_compound",
           "hubmapbags_collection_disease",
           "hubmapbags_collection_gene",
           "hubmapbags_collection_phenotype",
           "hubmapbags_collection_protein",
           "hubmapbags_collection_substance",
           "hubmapbags_collection_taxonomy",
           "hubmapbags_collection_in_collection",
           "hubmapbags_file_format",
           ]

sys.path.pop()
sys.path.pop()
sys.path.pop()
