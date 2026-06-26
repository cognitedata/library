"""Default configuration for the inverted index implementation."""

from __future__ import annotations

INDEX_STORAGE_CONFIG: dict = {
    "backend": "raw",
    "dm": {
        "space": "contextualization_idx",
        "view": "InvertedIndexEntry",
        "version": "v1",
    },
    "raw": {
        "database": "db_contextualization_idx",
        "table_template": "inverted_index__{scope_slug}",
        "row_key_template": "{match_scope_key}::{normalized_term}",
        "postings_column": "POSTINGS_JSON",
        "registry_table": "inverted_index__registry",
    },
}

RAW_SCOPE_POLICY: dict = {
    "min_levels": ["site", "unit"],
    "warn_rows_per_partition": 300_000,
    "alert_rows_per_partition": 400_000,
    "hard_max_rows_per_partition": 500_000,
}

RAW_POSTINGS_POLICY: dict = {
    "max_postings_per_row": 500,
    "warn_postings_per_row": 200,
    "index_store_vertices": False,
    "warn_postings_json_bytes": 262_144,
    "alert_postings_json_bytes": 524_288,
}

TARGET_DRIVEN_DEDUPE_CONFIG: dict = {
    "enabled": True,
    "cooldown_seconds": 300,
    "raw_database": "db_contextualization_idx",
    "state_table": "target_driven_state",
    "dedupe_key_fields": [
        "instance_space",
        "instance_external_id",
        "aliases_hash",
        "match_scope_key",
    ],
}

SCOPE_CONFIG: dict = {
    "enabled": False,
    "levels": [],
    "scope_key_template": "site:{site}|unit:{unit}",
    "resolve_from": {},
    "resolve_from_default": {},
    "resolve_from_examples": {
        "CogniteAsset": {
            "site": ["metadata.site", "siteId"],
            "unit": ["metadata.unit", "unitCode"],
        },
        "CogniteFile": {
            "site": ["metadata.site"],
            "unit": ["metadata.unit"],
        },
        "CogniteEquipment": {
            "site": ["metadata.site"],
            "unit": ["metadata.unit"],
        },
        "CogniteTimeSeries": {
            "site": ["metadata.site"],
            "unit": ["metadata.unit"],
        },
        "CogniteDiagramAnnotation": {
            "site": ["metadata.site"],
            "unit": ["metadata.unit"],
        },
    },
    "annotation_scope_via_linked_file": True,
    "strict_scope": False,
    "fallback_scope_key": "global",
}

INDEX_FIELD_CONFIG: list[dict] = [
    {
        "view": "CogniteFile",
        "view_space": "cdf_cdm",
        "version": "v1",
        "properties": [
            {"path": "name", "source_type": "file_metadata"},
            {
                "path": "description",
                "source_type": "asset_metadata",
                "extract_mode": "regex",
                "extract_pattern": r"\b[A-Z]{1,2}-\d{3,4}[A-Z]?\b",
            },
        ],
    },
    {
        "view": "CogniteEquipment",
        "view_space": "cdf_cdm",
        "version": "v1",
        "properties": [
            {"path": "name", "source_type": "asset_metadata"},
            {
                "path": "description",
                "source_type": "asset_metadata",
                "extract_mode": "regex",
                "extract_pattern": r"\b[A-Z]{1,2}-\d{3,4}[A-Z]?\b",
            },
        ],
    },
    {
        "view": "CogniteTimeSeries",
        "view_space": "cdf_cdm",
        "version": "v1",
        "properties": [
            {"path": "name", "source_type": "asset_metadata"},
            {
                "path": "description",
                "source_type": "asset_metadata",
                "extract_mode": "regex",
                "extract_pattern": r"\b[A-Z]{1,2}-\d{3,4}[A-Z]?\b",
            },
        ],
    },
]

ANNOTATION_INDEX_CONFIG: dict = {
    "view": "CogniteDiagramAnnotation",
    "view_space": "cdf_cdm",
    "version": "v1",
    "instance_type": "edge",
    "text_property": "startNodeText",
    "confidence_property": "confidence",
    "status_property": "status",
    "page_property": "startNodePageNumber",
    "bbox_properties": [
        "startNodeXMin",
        "startNodeYMin",
        "startNodeXMax",
        "startNodeYMax",
    ],
    "detection_mode_property": None,
    "detection_mode_tags": {
        "pattern": ["pattern", "pattern_mode"],
        "standard": ["standard", "tag", "tag_detection"],
    },
    "default_detection_mode": "pattern",
}

SUBSCRIPTION_CONFIG: dict = {
    "enabled": True,
    "trigger": "instance_subscription",
    "watch_property": "aliases",
    "instance_spaces": ["cdf_cdm"],
    "asset_views": ["CogniteAsset"],
    "file_views": ["CogniteFile"],
    "default_instance_type": "asset",
}

VIEW_INSTANCE_TYPES: dict[str, str] = {
    "CogniteAsset": "asset",
    "CogniteFile": "file",
    "CogniteEquipment": "equipment",
    "CogniteTimeSeries": "timeseries",
}

_INCOMING_INSTANCE = {"source": "incoming_instance"}

_FILE_FROM_HIT_RULES = [
    {
        "when_reference_types": ["CogniteFile"],
        "space": "reference_space",
        "external_id": "reference_external_id",
    },
    {
        "when_reference_types": ["CogniteDiagramAnnotation"],
        "space": "additional_metadata.file_space",
        "external_id": "additional_metadata.file_external_id",
        "fallback": {
            "space": "additional_metadata.file_space",
            "external_id": "additional_metadata.linked_file_extid",
        },
    },
]

_ASSET_FROM_HIT_RULES = [
    {
        "when_reference_types": ["CogniteAsset"],
        "space": "reference_space",
        "external_id": "reference_external_id",
    },
]

_EQUIPMENT_FROM_HIT_RULES = [
    {
        "when_reference_types": ["CogniteEquipment", "MyCustomEquipment"],
        "space": "reference_space",
        "external_id": "reference_external_id",
    },
    {
        "when_reference_types": ["CogniteDiagramAnnotation"],
        "space": "additional_metadata.equipment_space",
        "external_id": "additional_metadata.linked_equipment_extid",
        "fallback": {
            "space": "reference_space",
            "external_id": "additional_metadata.linked_equipment_extid",
        },
    },
]

_TIMESERIES_FROM_HIT_RULES = [
    {
        "when_reference_types": ["CogniteTimeSeries"],
        "space": "reference_space",
        "external_id": "reference_external_id",
    },
]

_DIAGRAM_ANNOTATION_DEFAULTS: dict = {
    "when_source_types": [
        "diagram_annotation_pattern",
        "diagram_annotation_standard",
    ],
    "annotation_id_path": "additional_metadata.annotation_external_id",
    "file_from_reference": True,
    "annotation_space_from": "reference_space",
    "create_status": "Suggested",
    "update_end_node_only": True,
    "property_map": {
        "startNodeText": "term",
        "confidence": "additional_metadata.confidence",
        "status": "additional_metadata.status",
        "startNodePageNumber": "additional_metadata.page",
    },
    "required_for_create": [
        "reference_external_id",
        "term",
        "additional_metadata.page",
    ],
}

DIRECT_RELATION_CONFIG: dict = {
    "enabled": True,
    "views": {
        "file": {"space": "cdf_cdm", "external_id": "CogniteFile", "version": "v1"},
        "asset": {"space": "cdf_cdm", "external_id": "CogniteAsset", "version": "v1"},
        "equipment": {
            "space": "cdf_cdm",
            "external_id": "CogniteEquipment",
            "version": "v1",
        },
        "timeseries": {
            "space": "cdf_cdm",
            "external_id": "CogniteTimeSeries",
            "version": "v1",
        },
        "diagram_annotation": {
            "space": "cdf_cdm",
            "external_id": "CogniteDiagramAnnotation",
            "version": "v1",
        },
    },
    "edge_views": {
        "file_asset_link": {
            "space": "cdf_cdm",
            "external_id": "FileAssetLink",
            "version": "v1",
        },
        "equipment_asset_link": {
            "space": "cdf_cdm",
            "external_id": "EquipmentAssetLink",
            "version": "v1",
        },
    },
    "links": {
        "file_to_asset": {
            "enabled": True,
            "write_modes": ["direct_relation"],
            "forward_view": "file",
            "property": "assets",
            "target_view": "asset",
            "cardinality": "list",
            "instance_types": ["asset", "file"],
            "source_types": [
                "diagram_annotation_pattern",
                "diagram_annotation_standard",
            ],
            "resolve_by_instance_type": {
                "asset": {
                    "forward": {"rules": _FILE_FROM_HIT_RULES},
                    "target": _INCOMING_INSTANCE,
                },
                "file": {
                    "forward": _INCOMING_INSTANCE,
                    "target": {"rules": _ASSET_FROM_HIT_RULES},
                },
            },
            "diagram_annotation": dict(_DIAGRAM_ANNOTATION_DEFAULTS),
        },
        "equipment_to_asset": {
            "enabled": True,
            "write_modes": ["direct_relation"],
            "forward_view": "equipment",
            "property": "asset",
            "target_view": "asset",
            "cardinality": "single",
            "overwrite_existing": False,
            "instance_types": ["asset", "equipment"],
            "source_types": ["asset_metadata", "diagram_annotation_pattern"],
            "resolve_by_instance_type": {
                "asset": {
                    "forward": {"rules": _EQUIPMENT_FROM_HIT_RULES},
                    "target": _INCOMING_INSTANCE,
                },
                "equipment": {
                    "forward": _INCOMING_INSTANCE,
                    "target": {"rules": _ASSET_FROM_HIT_RULES},
                },
            },
        },
        "equipment_to_file": {
            "enabled": True,
            "write_modes": ["direct_relation"],
            "forward_view": "equipment",
            "property": "files",
            "target_view": "file",
            "cardinality": "list",
            "instance_types": ["asset", "equipment", "file"],
            "source_types": ["asset_metadata", "file_metadata"],
            "resolve_by_instance_type": {
                "asset": {
                    "forward": {"rules": _EQUIPMENT_FROM_HIT_RULES},
                    "target": {"rules": _FILE_FROM_HIT_RULES},
                },
                "equipment": {
                    "forward": _INCOMING_INSTANCE,
                    "target": {"rules": _FILE_FROM_HIT_RULES},
                },
                "file": {
                    "forward": {"rules": _EQUIPMENT_FROM_HIT_RULES},
                    "target": _INCOMING_INSTANCE,
                },
            },
        },
        "timeseries_to_asset": {
            "enabled": True,
            "write_modes": ["direct_relation"],
            "forward_view": "timeseries",
            "property": "assets",
            "target_view": "asset",
            "cardinality": "list",
            "instance_types": ["asset", "timeseries"],
            "source_types": ["asset_metadata"],
            "resolve_by_instance_type": {
                "asset": {
                    "forward": {"rules": _TIMESERIES_FROM_HIT_RULES},
                    "target": _INCOMING_INSTANCE,
                },
                "timeseries": {
                    "forward": _INCOMING_INSTANCE,
                    "target": {"rules": _ASSET_FROM_HIT_RULES},
                },
            },
        },
        "timeseries_to_equipment": {
            "enabled": True,
            "write_modes": ["direct_relation"],
            "forward_view": "timeseries",
            "property": "equipment",
            "target_view": "equipment",
            "cardinality": "single",
            "overwrite_existing": False,
            "instance_types": ["equipment", "timeseries"],
            "source_types": ["asset_metadata"],
            "resolve_by_instance_type": {
                "equipment": {
                    "forward": {"rules": _TIMESERIES_FROM_HIT_RULES},
                    "target": _INCOMING_INSTANCE,
                },
                "timeseries": {
                    "forward": _INCOMING_INSTANCE,
                    "target": {
                        "rules": [
                            {
                                "when_reference_types": ["CogniteEquipment"],
                                "space": "reference_space",
                                "external_id": "reference_external_id",
                            },
                        ]
                    },
                },
            },
        },
    },
    "min_confidence": 0.6,
    "require_annotation_status": None,
    "allowed_annotation_statuses": ["Suggested", "Approved"],
    "write_on_suggested_annotations": True,
    "source_types": [
        "diagram_annotation_pattern",
        "diagram_annotation_standard",
        "asset_metadata",
        "file_metadata",
    ],
    "max_list_size": 1000,
}

FRESHNESS_CONFIG: dict = {
    "max_index_age_seconds": 900,
    "check_before_target_driven": True,
    "on_stale": "warn_and_continue",
}

TAG_REUSE_AUDIT_POLICY: dict = {
    "warn_scope_count": 50,
}
