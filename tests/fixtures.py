slack_upload_reorg_priority_str = [
    "Priority upload (test_priority_project) reorganized:",
    "hubmap_id: <https://ingest.hubmapconsortium.org/Upload/test_uuid|test_hm_id>",
    "created_by_user_displayname: Test User",
    "created_by_user_email: test@user.com",
    "dataset_type: test_dataset_type",
    "organ: test_organ",
    "priority_project_list: test_priority_project",
    "",
    "Datasets:",
    "hubmap_id,created_by_user_displayname,created_by_user_email,priority_project_list,dataset_type,organ,globus_link,filesystem_path",
    "test_dataset_hm_id, test user, test@user.com, test_priority_project, test_dataset_type, test_organ, <test_globus_url|Globus>, test_abs_path",
]


slack_upload_reorg_str = [
    "Upload test_uuid reorganized:",
    "hubmap_id: <https://ingest.hubmapconsortium.org/Upload/test_uuid|test_hm_id>",
    "created_by_user_displayname: Test User",
    "created_by_user_email: test@user.com",
    "dataset_type: test_dataset_type",
    "organ: test_organ",
    "",
    "Datasets:",
    "hubmap_id,created_by_user_displayname,created_by_user_email,dataset_type,organ,globus_link,filesystem_path",
    "test_dataset_hm_id, test user, test@user.com, test_dataset_type, test_organ, <test_globus_url|Globus>, test_abs_path",
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
            "created_by_user_displayname": "Test User",
            "created_by_user_email": "test@user.com",
            "priority_project_list": ["test_priority_project"],
            "dataset_type": "test_dataset_type",
            "metadata": [{"parent_dataset_id": "test_parent_id"}],
        }
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

validation_report = 'Data File Errors:\n  Recursively test all ome-tiff files for an assay-specific list of fields:\n  - \'/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_L/lab_processed/images/HUBMAP_H&E_Slide99_C31C1.ome.tiff\n    is not a valid OME.TIFF file: Failed to read OME XML.\'\n  - \'/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_R/lab_processed/images/HUBMAP_H&E_Slide99_C32C1.ome.tiff\n    is not a valid OME.TIFF file: Failed to read OME XML.\'\n  Recursively test all ome-tiff files for validity:\n  - "/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_L/lab_processed/images/HUBMAP_H&E_Slide99_C31C1.ome.tiff\\\n    \\ is not a valid OME.TIFF file: XMLSchemaDecodeError: attribute Color="(255, 0, 0)": invalid literal for int()\\\n    \\ with base 10: "(255, 0, 0)"\n  - "/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_R/lab_processed/images/HUBMAP_H&E_Slide99_C32C1.ome.tiff\\\n    \\ is not a valid OME.TIFF file: XMLSchemaDecodeError: attribute Color="(255, 0, 0)": invalid literal for int()\\\n    \\ with base 10: "(255, 0, 0)"\n'

validation_report_lines = [
    "Data File Errors:",
    "Recursively test all ome-tiff files for an assay-specific list of fields:",
    "- '/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_L/lab_processed/images/HUBMAP_H&E_Slide99_C31C1.ome.tiff",
    "is not a valid OME.TIFF file: Failed to read OME XML.'",
    "- '/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_R/lab_processed/images/HUBMAP_H&E_Slide99_C32C1.ome.tiff",
    "is not a valid OME.TIFF file: Failed to read OME XML.'",
    "Recursively test all ome-tiff files for validity:",
    '- "/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_L/lab_processed/images/HUBMAP_H&E_Slide99_C31C1.ome.tiff is not a valid OME.TIFF file: XMLSchemaDecodeError: attribute Color="(255, 0, 0)": invalid literal for int() with base 10: "(255, 0, 0)"',
    '- "/hive/hubmap/data/protected/Stanford University Bone Marrow TMC/f6aff8e28f2fc0ef22e71cbadc1f6f51/HUBMAP1_slide100_R/lab_processed/images/HUBMAP_H&E_Slide99_C32C1.ome.tiff is not a valid OME.TIFF file: XMLSchemaDecodeError: attribute Color="(255, 0, 0)": invalid literal for int() with base 10: "(255, 0, 0)"',
]
