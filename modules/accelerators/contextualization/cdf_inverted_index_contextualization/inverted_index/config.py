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

RAW_TERM_PARTITION_POLICY: dict = {
    "enabled": False,
    "strategy": "first_char",
    "activate_above_rows": 400_000,
    "sharded_table_template": "inverted_index__{scope_slug}__{term_bucket}",
    "bucket_mode": "script_aware",
}

PARTITION_STRATEGY_UNIFIED = "unified"
PARTITION_STRATEGY_TERM_FIRST_CHAR = "term_first_char"

TARGET_DRIVEN_CONFIG: dict = {
    "query_property": "aliases",
    "query_property_fallbacks": ["name"],
    "exclude_empty_aliases": False,
}

TARGET_DRIVEN_DEDUPE_CONFIG: dict = {
    "enabled": True,
    "cooldown_seconds": 300,
    "raw_database": "db_contextualization_idx",
    "state_table": "target_driven_state",
    "dedupe_key_fields": [
        "instance_space",
        "instance_external_id",
        "terms_hash",
        "match_scope_key",
    ],
}

SCOPE_CONFIG: dict = {
    "enabled": True,
    "levels": [],
    "scope_key_template": "site:{site}|unit:{unit}",
    "resolve_from": {},
    "resolve_from_default": {},
    "resolve_from_examples": {
        "CogniteAsset": {
            "site": ["sourceContext", "source"],
            "unit": ["sourceId"],
        },
        "CogniteFile": {
            "site": ["sourceContext"],
            "unit": ["sourceId"],
        },
        "CogniteEquipment": {
            "site": ["sourceContext"],
            "unit": ["sourceId"],
        },
        "CogniteTimeSeries": {
            "site": ["sourceContext"],
            "unit": ["sourceId"],
        },
        "CogniteDiagramAnnotation": {
            "site": ["sourceContext"],
            "unit": ["sourceId"],
        },
    },
    "annotation_scope_via_linked_file": True,
    "strict_scope": False,
    "fallback_scope_key": "global",
}

_INDEX_TAG_PATTERN = r"\b[A-Z]{1,2}-\d{3,4}[A-Z]?\b"
_INDEX_FILE_EXT_PATTERN = r"(?i)\b[\w][\w.-]*\.(?:pdf|dwg|png|jpe?g|tif{1,2})\b"
# Drawing / master-document IDs without file extension (prior revision, master P&ID, etc.)
_INDEX_DOC_REF_PATTERN = r"\b[A-Z]{2,}(?:-[A-Z]{2,})*-P-\d{4}(?:-\d{3})?\b"


def view_query_instance_spaces(view_config: dict) -> list[str | None]:
    """Resolve DM instance spaces for a metadata ``index_field_config`` view entry."""
    raw = view_config.get("instance_spaces")
    if isinstance(raw, list) and raw:
        return [str(s).strip() for s in raw if str(s).strip()]
    return [None]


_INDEX_METADATA_PROPERTIES: list[dict] = [
    {
        "path": "name",
        "source_type": "asset_metadata",
        "extract_mode": "regex",
        "extract_pattern": _INDEX_TAG_PATTERN,
    },
    {
        "path": "description",
        "source_type": "asset_metadata",
        "extract_mode": "regex",
        "extract_pattern": _INDEX_TAG_PATTERN,
    },
    {
        "path": "description",
        "source_type": "file_metadata",
        "extract_mode": "regex",
        "extract_pattern": _INDEX_FILE_EXT_PATTERN,
    },
    {
        "path": "description",
        "source_type": "file_metadata",
        "extract_mode": "regex",
        "extract_pattern": _INDEX_DOC_REF_PATTERN,
    },
]

def _index_view_entry(view: str) -> dict:
    return {
        "view": view,
        "view_space": "cdf_cdm",
        "version": "v1",
        "instance_spaces": [],
        "filters": [],
        "properties": list(_INDEX_METADATA_PROPERTIES),
    }


INDEX_FIELD_CONFIG: list[dict] = [
    _index_view_entry("CogniteFile"),
    _index_view_entry("CogniteAsset"),
    _index_view_entry("CogniteEquipment"),
    _index_view_entry("CogniteTimeSeries"),
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
    "instance_spaces": [],
    "watch_view_keys": ["asset", "file"],
}

FRESHNESS_CONFIG: dict = {
    "max_index_age_seconds": 900,
    "check_before_target_driven": True,
    "on_stale": "warn_and_continue",
}

TAG_REUSE_AUDIT_POLICY: dict = {
    "warn_scope_count": 50,
}

VIRTUAL_TAG_CREATION_CONFIG: dict = {
    "enabled": False,
    "incremental_enabled": True,
    "term_selection_mode": "missing_tags_only",
    "source_types": ["asset_metadata", "diagram_annotation_pattern"],
    "missing_tag_criteria": {
        "require_pattern_detection": True,
        "check_existing_cognite_asset": True,
        "exclude_with_cognite_asset_metadata": True,
    },
    "asset_lookup": {
        "view_external_id": "CogniteAsset",
        "view_space": "cdf_cdm",
        "view_version": "v1",
        "instance_spaces": [],
        "match_properties": ["name", "aliases"],
        "scope_filter": True,
    },
    "hierarchy_levels": [],
    "leaf_level": "asset_tag",
    "instance_space": "inst_virtual_tags",
    "view_space": "cdf_cdm",
    "view_external_id": "CogniteAsset",
    "view_version": "v1",
    "populate_aliases": True,
    "scope_property_mapping": {
        "site": "sourceContext",
        "unit": "sourceId",
    },
    "min_confidence": 0.0,
    "batch_limit": 0,
    "skip_existing": False,
    "apply_chunk_size": 500,
}
