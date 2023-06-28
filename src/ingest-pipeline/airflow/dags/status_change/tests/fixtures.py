custom_submission_data_fixture = {
    "datasets": [
        {
            "created_timestamp": 1628179023287,
            "data_types": ["test_type"],
            "entity_type": "Dataset",
            "group_name": "test_group",
            "hubmap_id": "test_hubmap_id",
            "ingest_metadata": {"metadata": {"assay_type": "test_assay_type"}},
            "last_modified_timestamp": 1652239003307,
            "status": "New",
            "uuid": "test_uuid",
        }
    ],
}


search_tasks_fixture_valid = [
    {
        "gid": "test_task_gid_1",
        "custom_fields": [
            {
                "gid": "custom_fields_2_gid",
                "name": "Entity Type",
                "enum_value": {"name": "Dataset"},
            }
        ],
    },
    {
        "gid": "test_task_gid_2",
        "custom_fields": [
            {
                "gid": "custom_fields_3_gid",
                "name": "Entity Type",
                "enum_value": {"name": "Upload"},
            }
        ],
    },
]
search_tasks_fixture_invalid = [
    {
        "gid": "test_task_gid_1",
        "custom_fields": [{"gid": "custom_fields_1_gid", "name": "Test Custom Field Name"}],
    },
    {
        "gid": "test_task_gid_2",
        "custom_fields": [
            {
                "gid": "custom_fields_2_gid",
                "name": "Entity Type",
                "enum_value": {"name": "Dataset"},
            }
        ],
    },
    {
        "gid": "test_task_gid_3",
        "custom_fields": [
            {
                "gid": "custom_fields_3_gid",
                "name": "Entity Type",
                "enum_value": {"name": "Dataset"},
            }
        ],
    },
]

custom_fields_fixture = [
    {
        "gid": "1204584344373112",
        "custom_field": {
            "gid": "1204584344373110",
            "name": "HuBMAP ID",
            "resource_subtype": "text",
            "resource_type": "custom_field",
            "type": "text",
            "is_formula_field": False,
        },
        "is_important": True,
        "parent": {
            "gid": "1204583312696119",
            "name": "Data Ingest",
            "resource_type": "project",
        },
        "project": {
            "gid": "1204583312696119",
            "name": "Data Ingest",
            "resource_type": "project",
        },
        "resource_type": "custom_field_setting",
    },
    {
        "gid": "1204584347884586",
        "custom_field": {
            "gid": "1204584344373114",
            "enum_options": [
                {
                    "gid": "1204584344373115",
                    "color": "cool-gray",
                    "enabled": True,
                    "name": "Intake",
                    "resource_type": "enum_option",
                },
                {
                    "gid": "1204584344373116",
                    "color": "yellow-orange",
                    "enabled": True,
                    "name": "Pre-Processing",
                    "resource_type": "enum_option",
                },
                {
                    "gid": "1204584347884579",
                    "color": "yellow-green",
                    "enabled": True,
                    "name": "Ready Backlog",
                    "resource_type": "enum_option",
                },
                {
                    "gid": "1204584347884580",
                    "color": "aqua",
                    "enabled": True,
                    "name": "Processing",
                    "resource_type": "enum_option",
                },
                {
                    "gid": "1204584347884581",
                    "color": "blue",
                    "enabled": True,
                    "name": "Post-Processing",
                    "resource_type": "enum_option",
                },
                {
                    "gid": "1204584347884582",
                    "color": "purple",
                    "enabled": True,
                    "name": "Publishing",
                    "resource_type": "enum_option",
                },
                {
                    "gid": "1204584347884583",
                    "color": "red",
                    "enabled": True,
                    "name": "Blocked 🛑",
                    "resource_type": "enum_option",
                },
                {
                    "gid": "1204584347884584",
                    "color": "cool-gray",
                    "enabled": True,
                    "name": "Completed",
                    "resource_type": "enum_option",
                },
            ],
            "name": "Process Stage",
            "resource_subtype": "enum",
            "resource_type": "custom_field",
            "type": "enum",
            "is_formula_field": False,
        },
        "is_important": True,
        "parent": {
            "gid": "1204583312696119",
            "name": "Data Ingest",
            "resource_type": "project",
        },
        "project": {
            "gid": "1204583312696119",
            "name": "Data Ingest",
            "resource_type": "project",
        },
        "resource_type": "custom_field_setting",
    },
    # Constructed data follows
    {
        "gid": "1204584344373111",
        "custom_field": {
            "gid": "1204584344373113",
            "name": "Parent",
            "resource_subtype": "text",
            "resource_type": "custom_field",
            "type": "text",
            "is_formula_field": False,
        },
        "is_important": True,
        "parent": {
            "gid": "1204583312696119",
            "name": "Data Ingest",
            "resource_type": "project",
        },
        "project": {
            "gid": "1204583312696119",
            "name": "Data Ingest",
            "resource_type": "project",
        },
        "resource_type": "custom_field_setting",
    },
    {
        "gid": "1204584347884585",
        "custom_field": {
            "gid": "1204584344373117",
            "enum_options": [
                {
                    "gid": "1204584344373118",
                    "enabled": True,
                    "name": "Dataset",
                    "resource_type": "enum_option",
                },
                {
                    "gid": "1204584344373119",
                    "enabled": True,
                    "name": "Upload",
                    "resource_type": "enum_option",
                },
            ],
            "name": "Entity Type",
            "resource_subtype": "enum",
            "resource_type": "custom_field",
            "type": "enum",
            "is_formula_field": False,
        },
        "is_important": True,
        "parent": {
            "gid": "1204583312696119",
            "name": "Data Ingest",
            "resource_type": "project",
        },
        "project": {
            "gid": "1204583312696119",
            "name": "Data Ingest",
            "resource_type": "project",
        },
        "resource_type": "custom_field_setting",
    },
]

task_updated_fixture = {
    "gid": "test_task_updated_gid",
    "custom_fields": [
        {
            "gid": "test_task_updated_status_gid",
            "resource_type": "custom_field",
            "type": "enum",
            "name": "Status",
            "enum_value": {
                "gid": "1204584344373116",
                "resource_type": "enum_option",
                "name": "Pre-Processing",
            },
            "display_value": "Valid",
        },
    ],
}

task_updated_reorg_fixture = {
    "custom_fields": [
        {"name": "HuBMAP ID"},
        {
            "name": "Status",
            "enum_value": {"gid": "1204584347884580", "name": "Processing"},
        },
    ]
}

datasets_fixture = {
    "datasets": [
        {
            "created_timestamp": 1628179023287,
            "data_types": ["test_type"],
            "entity_type": "Dataset",
            "hubmap_id": "test_hubmap_id",
            "last_modified_timestamp": 1652239003307,
            "status": "New",
            "uuid": "test_uuid",
        }
    ]
}

datasets_bad_timestamp_fixture = {
    "datasets": [
        {
            "created_timestamp": 1628179023287,
            "data_types": ["test_type"],
            "entity_type": "Dataset",
            "hubmap_id": "test_hubmap_id",
            "last_modified_timestamp": 1652239003307,
            "status": "New",
            "uuid": "test_uuid",
        }
    ]
}
