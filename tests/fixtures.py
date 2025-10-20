slack_upload_reorg_priority_str = [
    "Priority upload (test_priority_project) reorganized:",
    "hubmap_id: <https://ingest.hubmapconsortium.org/test_uuid|None>",
    "created_by_user_displayname: None",
    "created_by_user_email: None",
    "dataset_type: test_type",
    "organ: test_organ",
    "priority_project_list: test_priority_project",
    "Datasets:",
    "hubmap_id,created_by_user_displayname,created_by_user_email,priority_project_list,dataset_type,organ,globus_link,filesystem_path",
    "test_hubmap_id, test_user, test_email, test_priority_project, test_type, test_organ, <https://app.globus.org/file-manager?origin_id=24c2ee95-146d-4513-a1b3-ac0bfdb7856f&origin_path=test_abs_path%2F|Globus>, test_abs_path",
]


slack_upload_reorg_str = [
    "Upload test_uuid reorganized:",
    "hubmap_id: <https://ingest.hubmapconsortium.org/test_uuid|None>",
    "created_by_user_displayname: None",
    "created_by_user_email: None",
    "dataset_type: test_type",
    "organ: test_organ",
    "Datasets:",
    "hubmap_id,created_by_user_displayname,created_by_user_email,dataset_type,organ,globus_link,filesystem_path",
    "test_hubmap_id, test_user, test_email, test_type, test_organ, <https://app.globus.org/file-manager?origin_id=24c2ee95-146d-4513-a1b3-ac0bfdb7856f&origin_path=test_abs_path%2F|Globus>, test_abs_path",
]

good_upload_context = {
    "validation_message": "existing validation_message text",
    "ingest_task": "existing ingest_task text",
    "unrelated_field": True,
    "status": "new",
    "entity_type": "Upload",
}
upload_context_mock_value = {
    "uuid": "test_uuid",
    "hubmap_id": "test_hm_id",
    "created_by_user_displayname": "Test User",
    "created_by_user_email": "test@user.com",
    "priority_project_list": ["test_project, test_project_2"],
    "datasets": [
        {
            "uuid": "test_dataset_uuid",
            "hubmap_id": "test_dataset_hm_id",
            "created_by_user_displayname": "Test User",
            "created_by_user_email": "test@user.com",
            "priority_project_list": ["test_project", "test_project_2"],
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
    "created_by_user_email": "test@user.com",
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
