slack_upload_reorg_priority_str = [
    "Priority upload (test_priority_project) reorganized:",
    "hubmap_id: <https://ingest.hubmapconsortium.org/upload/test_uuid|test_hm_id>",
    "created_by_user_displayname: Test User",
    "created_by_user_email: test@user.com",
    "dataset_type: test_dataset_type",
    "organ: test_organ",
    "priority_project_list: test_priority_project",
    "",
    "Datasets:",
    "hubmap_id,created_by_user_displayname,created_by_user_email,priority_project_list,dataset_type,organ,globus_link,filesystem_path",
    "test_dataset_hm_id, test user, test@user.com, test_priority_project, test_dataset_type, test_organ, <test_globus_url|Globus>, test_abs_path",
    "test_dataset_hm_id2, test user2, test@user2.com, test_priority_project, test_dataset_type2, test_organ, <test_globus_url|Globus>, test_abs_path",
]


slack_upload_reorg_str = [
    "Upload test_uuid reorganized:",
    "hubmap_id: <https://ingest.hubmapconsortium.org/upload/test_uuid|test_hm_id>",
    "created_by_user_displayname: Test User",
    "created_by_user_email: test@user.com",
    "dataset_type: test_dataset_type",
    "organ: test_organ",
    "",
    "Datasets:",
    "hubmap_id,created_by_user_displayname,created_by_user_email,dataset_type,organ,globus_link,filesystem_path",
    "test_dataset_hm_id, test user, test@user.com, test_dataset_type, test_organ, <test_globus_url|Globus>, test_abs_path",
    "test_dataset_hm_id2, test user2, test@user2.com, test_dataset_type2, test_organ, <test_globus_url|Globus>, test_abs_path",
]

good_upload_context = {
    "uuid": "test_uuid",
    "hubmap_id": "test_hm_id",
    "created_by_user_displayname": "Test User",
    "created_by_user_email": "test@user.com",
    "group_name": "test group",
    "priority_project_list": ["test_priority_project"],
    "unrelated_field": True,
    "ingest_task": "existing ingest_task text",
    "validation_message": "existing validation_message text",
    "datasets": [
        {
            "uuid": "test_dataset_uuid",
            "hubmap_id": "test_dataset_hm_id",
            "entity_type": "Dataset",
            "created_by_user_displayname": "Test User",
            "created_by_user_email": "test@user.com",
            "priority_project_list": ["test_priority_project"],
            "dataset_type": "test_dataset_type",
            "metadata": [{"parent_dataset_id": "test_parent_id"}],
            "status": "Submitted",
        },
        {
            "uuid": "test_dataset_uuid2",
            "hubmap_id": "test_dataset_hm_id2",
            "entity_type": "Dataset",
            "created_by_user_displayname": "Test User2",
            "created_by_user_email": "test@user2.com",
            "priority_project_list": ["test_priority_project"],
            "dataset_type": "test_dataset_type2",
            "metadata": [{"parent_dataset_id": "test_parent_id"}],
            "status": "Submitted",
        },
    ],
    "status": "New",
    "entity_type": "Upload",
}

dataset_context_mock_value = {
    "uuid": "test_dataset_uuid",
    "hubmap_id": "test_hm_dataset_id",
    "created_by_user_displayname": "Test User",
    "created_by_user_email": "test_dataset@user.com",
    "status": "New",
    "entity_type": "Dataset",
}

endpoints = {
    "hubmap": {
        "PROD": {
            "entity_url": "https://entity.api.hubmapconsortium.org",
            "ingest_url": "vm004@hive.psc.edu",
        },
        "DEV": {"entity_url": "https://entity-api.dev.hubmapconsortium.org"},
    },
    "sennet": {
        "PROD": {"entity_url": "https://entity.api.sennetconsortium.org"},
        "DEV": {"entity_url": "https://entity-api.dev.sennetconsortium.org"},
    },
}

# validation_report = 'Data File Errors:\n  Recursively test all ome-tiff files for an assay-specific list of fields:\n  - \'/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_L/lab_processed/images/HUBMAP_H&E_Slide99_C31C1.ome.tiff\n    is not a valid OME.TIFF file: Failed to read OME XML.\'\n  - \'/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_R/lab_processed/images/HUBMAP_H&E_Slide99_C32C1.ome.tiff\n    is not a valid OME.TIFF file: Failed to read OME XML.\'\n  Recursively test all ome-tiff files for validity:\n  - "/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_L/lab_processed/images/HUBMAP_H&E_Slide99_C31C1.ome.tiff\\\n    \\ is not a valid OME.TIFF file: XMLSchemaDecodeError: attribute Color="(255, 0, 0)": invalid literal for int()\\\n    \\ with base 10: "(255, 0, 0)"\n  - "/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_R/lab_processed/images/HUBMAP_H&E_Slide99_C32C1.ome.tiff\\\n    \\ is not a valid OME.TIFF file: XMLSchemaDecodeError: attribute Color="(255, 0, 0)": invalid literal for int()\\\n    \\ with base 10: "(255, 0, 0)"\n'

validation_report_html_list = [
    "<ul>",
    "<li>Directory Errors:</li>",
    "<ul>",
    "<li>examples/dataset-examples/bad-scatacseq-data/upload/dataset-1 (as scatacseq-v0.0):</li>",
    "<ul>",
    "<li>Not allowed:</li>",
    "<ul>",
    "<li>not-the-file-you-are-looking-for.txt</li>",
    "<li>unexpected-directory/place-holder.txt</li>",
    "</ul>",
    "<li>Required but missing:</li>",
    "<ul>",
    "<li>[^/]+\\.fastq\\.gz</li>",
    "</ul>",
    "</ul>",
    "</ul>",
    "<li>Antibodies/Contributors Errors:</li>",
    "<ul>",
    "<li>examples/dataset-examples/bad-scatacseq-data/upload/scatacseq-metadata.tsv:</li>",
    "<ul>",
    '<li>On row(s) 2, column "contributors_path", error opening or reading value ".". Expected a TSV, but found a directory: examples/dataset-examples/bad-scatacseq-data/upload.</li>',
    "</ul>",
    "</ul>",
    "<li>Local Validation Errors:</li>",
    "<ul>",
    "<li>examples/dataset-examples/bad-scatacseq-data/upload/scatacseq-metadata.tsv (as scatacseq-v0):</li>",
    "<ul>",
    '<li>On row 2, column "sc_isolation_protocols_io_doi", value "" fails because it must be filled out.</li>',
    '<li>On row 2, column "library_construction_protocols_io_doi", value "" fails because it must be filled out.</li>',
    '<li>On row 2, column "protocols_io_doi", value "10.17504/fake" fails because it is an invalid DOI.</li>',
    "</ul>",
    "</ul>",
    "</ul>",
]

ext_error_formatted = [
    "HuBMAP Upload <a href='https://ingest.hubmapconsortium.org/Upload/test_uuid'>test_hm_id</a> has failed validation.",
    "",
    "<b>Validation details</b>",
    "The validation process starts by checking metadata TSVs and directory structures. If those checks pass, then certain individual file types (such as FASTQ and OME.TIFF files) are validated.",
    "",
    "<b>What to do next</b>",
    "If you have questions about your upload, please schedule an appointment with Data Curator Brendan Honick: https://calendly.com/bhonick-psc/.",
    "",
    "This email address is not monitored. If you have questions, please schedule with Brendan Honick or email ingest@hubmapconsortium.org.",
    "",
    "The error log is included below if you would like to make updates to your submission independently; it is not required for you to do so before contacting our Data Curation Team. Please email ingest@hubmapconsortium.org if you believe you have repaired all validation errors so that we can re-validate your submission.",
    "",
    'If your submission has "Spreadsheet Validator Errors," please use the <a href="https://metadatavalidator.metadatacenter.org/">Metadata Spreadsheet Validator</a> tool to correct them.',
    "",
    "<b>Validation error log</b>",
    *validation_report_html_list,
]

validation_error_dict = {
    "Directory Errors": {
        "examples/dataset-examples/bad-scatacseq-data/upload/dataset-1 (as scatacseq-v0.0)": {
            "Not allowed": [
                "not-the-file-you-are-looking-for.txt",
                "unexpected-directory/place-holder.txt",
            ],
            "Required but missing": ["[^/]+\\.fastq\\.gz"],
        }
    },
    "Antibodies/Contributors Errors": {
        "examples/dataset-examples/bad-scatacseq-data/upload/scatacseq-metadata.tsv": [
            'On row(s) 2, column "contributors_path", error opening or reading value ".". Expected a TSV, but found a directory: examples/dataset-examples/bad-scatacseq-data/upload.'
        ]
    },
    "Local Validation Errors": {
        "examples/dataset-examples/bad-scatacseq-data/upload/scatacseq-metadata.tsv (as scatacseq-v0)": [
            'On row 2, column "sc_isolation_protocols_io_doi", value "" fails because it must be filled out.',
            'On row 2, column "library_construction_protocols_io_doi", value "" fails because it must be filled out.',
            'On row 2, column "protocols_io_doi", value "10.17504/fake" fails because it is an invalid DOI.',
        ]
    },
}

ext_error = 'HuBMAP Upload <a href="https://ingest.hubmapconsortium.org/upload/test_uuid">test_hm_id</a> has failed validation.<br><br><b>Validation details</b><br>The validation process starts by checking metadata TSVs and directory structures. If those checks pass, then certain individual file types (such as FASTQ and OME.TIFF files) are validated.<br><br><b>What to do next</b><br>If you have questions about your upload, please schedule an appointment with Data Curator Brendan Honick (https://calendly.com/bhonick-psc/) or email ingest@hubmapconsortium.org. Do not respond to this email; this inbox is not monitored.<br><br>The error log is included below if you would like to make updates to your submission independently. It is not required for you to correct any errors before contacting our Data Curation Team. Please email ingest@hubmapconsortium.org if you believe you have repaired all validation errors so that we can re-validate your submission.<br><br>If your submission has "Spreadsheet Validator Errors," please use the <a href="https://metadatavalidator.metadatacenter.org/">Metadata Spreadsheet Validator</a> tool to correct them.<br><br><b>Validation error log</b><br><ul><li>Directory Errors:</li><ul><li>examples/dataset-examples/bad-scatacseq-data/upload/dataset-1 (as scatacseq-v0.0):</li><ul><li>Not allowed:</li><ul><li>not-the-file-you-are-looking-for.txt</li><li>unexpected-directory/place-holder.txt</li></ul><li>Required but missing:</li><ul><li>[^/]+\\.fastq\\.gz</li></ul></ul></ul><li>Antibodies/Contributors Errors:</li><ul><li>examples/dataset-examples/bad-scatacseq-data/upload/scatacseq-metadata.tsv:</li><ul><li>On row(s) 2, column "contributors_path", error opening or reading value ".". Expected a TSV, but found a directory: examples/dataset-examples/bad-scatacseq-data/upload.</li></ul></ul><li>Local Validation Errors:</li><ul><li>examples/dataset-examples/bad-scatacseq-data/upload/scatacseq-metadata.tsv (as scatacseq-v0):</li><ul><li>On row 2, column "sc_isolation_protocols_io_doi", value "" fails because it must be filled out.</li><li>On row 2, column "library_construction_protocols_io_doi", value "" fails because it must be filled out.</li><li>On row 2, column "protocols_io_doi", value "10.17504/fake" fails because it is an invalid DOI.</li></ul></ul></ul>'
