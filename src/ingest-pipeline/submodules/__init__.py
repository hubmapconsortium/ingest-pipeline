import sys
import os
from importlib import import_module

sys.path.append(os.path.join(os.path.dirname(__file__),
                             'ingest-validation-tools', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__),
                             'ingest-validation-tests', 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__),
                             'py-hubmapbags'))
sys.path.append(os.path.join(os.path.dirname(__file__),
                             'hubmap-inventory'))

ingest_validation_tools_upload = import_module('ingest_validation_tools.upload')
ingest_validation_tools_error_report = import_module('ingest_validation_tools.error_report')
ingest_validation_tools_validation_utils = import_module('ingest_validation_tools.validation_utils')
ingest_validation_tools_plugin_validator = import_module('ingest_validation_tools.plugin_validator')
ingest_validation_tests = import_module('ingest_validation_tests')
hubmapbags = import_module('hubmapbags')
hubmapbags_anatomy = import_module('hubmapbags.anatomy')
hubmapbags_apis = import_module('hubmapbags.apis')
hubmapbags_assay_type = import_module('hubmapbags.assay_type')
hubmapbags_biosample = import_module('hubmapbags.biosample')
hubmapbags_biosample_disease = import_module('hubmapbags.biosample_disease')
hubmapbags_biosample_from_subject = import_module('hubmapbags.biosample_from_subject')
hubmapbags_biosample_gene = import_module('hubmapbags.biosample_gene')
hubmapbags_biosample_in_collection = import_module('hubmapbags.biosample_in_collection')
hubmapbags_biosample_substance = import_module('hubmapbags.biosample_substance')
hubmapbags_collection = import_module('hubmapbags.collection')
hubmapbags_collection_anatomy = import_module('hubmapbags.collection_anatomy')
hubmapbags_collection_compound = import_module('hubmapbags.collection_compound')
hubmapbags_collection_defined_by_project = import_module('hubmapbags.collection_defined_by_project')
hubmapbags_collection_disease = import_module('hubmapbags.collection_disease')
hubmapbags_collection_gene = import_module('hubmapbags.collection_gene')
hubmapbags_collection_in_collection = import_module('hubmapbags.collection_in_collection')
hubmapbags_collection_phenotype = import_module('hubmapbags.collection_phenotype')
hubmapbags_collection_protein = import_module('hubmapbags.collection_protein')
hubmapbags_collection_substance = import_module('hubmapbags.collection_substance')
hubmapbags_collection_taxonomy = import_module('hubmapbags.collection_taxonomy')
hubmapbags_file = import_module('hubmapbags.file')
hubmapbags_file_describes_biosample = import_module('hubmapbags.file_describes_biosample')
hubmapbags_file_describes_collection = import_module('hubmapbags.file_describes_collection')
hubmapbags_file_describes_subject = import_module('hubmapbags.file_describes_subject')
hubmapbags_file_format = import_module('hubmapbags.file_format')
hubmapbags_file_in_collection = import_module('hubmapbags.file_in_collection')
hubmapbags_id_namespace = import_module('hubmapbags.id_namespace')
hubmapbags_magic = import_module('hubmapbags.magic')
hubmapbags_ncbi_taxonomy = import_module('hubmapbags.ncbi_taxonomy')
hubmapbags_primary_dcc_contact = import_module('hubmapbags.primary_dcc_contact')
hubmapbags_project_in_project = import_module('hubmapbags.project_in_project')
hubmapbags_projects = import_module('hubmapbags.projects')
hubmapbags_subject = import_module('hubmapbags.subject')
hubmapbags_subject_disease = import_module('hubmapbags.subject_disease')
hubmapbags_subject_in_collection = import_module('hubmapbags.subject_in_collection')
hubmapbags_subject_phenotype = import_module('hubmapbags.subject_phenotype')
hubmapbags_subject_race = import_module('hubmapbags.subject_race')
hubmapbags_subject_role_taxonomy = import_module('hubmapbags.subject_role_taxonomy')
hubmapbags_subject_substance = import_module('hubmapbags.subject_substance')
hubmapbags_utilities = import_module('hubmapbags.utilities')
hubmapbags_uuids = import_module('hubmapbags.uuids')
hubmapinventory = import_module('hubmapinventory')

__all__ = ["ingest_validation_tools_validation_utils",
           "ingest_validation_tools_upload",
           "ingest_validation_tools_error_report",
           "ingest_validation_tools_plugin_validator",
           "ingest_validation_tests",
           "hubmapbags",
           "hubmapbags_anatomy",
           "hubmapbags_apis",
           "hubmapbags_assay_type",
           "hubmapbags_biosample",
           "hubmapbags_biosample_disease",
           "hubmapbags_biosample_from_subject",
           "hubmapbags_biosample_gene",
           "hubmapbags_biosample_in_collection",
           "hubmapbags_biosample_substance",
           "hubmapbags_collection",
           "hubmapbags_collection_anatomy",
           "hubmapbags_collection_compound",
           "hubmapbags_collection_defined_by_project",
           "hubmapbags_collection_disease",
           "hubmapbags_collection_gene",
           "hubmapbags_collection_in_collection",
           "hubmapbags_collection_phenotype",
           "hubmapbags_collection_protein",
           "hubmapbags_collection_substance",
           "hubmapbags_collection_taxonomy",
           "hubmapbags_file",
           "hubmapbags_file_describes_biosample",
           "hubmapbags_file_describes_collection",
           "hubmapbags_file_describes_subject",
           "hubmapbags_file_format",
           "hubmapbags_file_in_collection",
           "hubmapbags_id_namespace",
           "hubmapbags_magic",
           "hubmapbags_ncbi_taxonomy",
           "hubmapbags_primary_dcc_contact",
           "hubmapbags_project_in_project",
           "hubmapbags_projects",
           "hubmapbags_subject",
           "hubmapbags_subject_disease",
           "hubmapbags_subject_in_collection",
           "hubmapbags_subject_phenotype",
           "hubmapbags_subject_race",
           "hubmapbags_subject_role_taxonomy",
           "hubmapbags_subject_substance",
           "hubmapbags_utilities",
           "hubmapbags_uuids",
           "hubmapinventory",
           ]

sys.path.pop()
sys.path.pop()
sys.path.pop()
sys.path.pop()
