"""Unit tests for metadata index entry build."""

from inverted_index.config import INDEX_FIELD_CONFIG, SCOPE_CONFIG
from inverted_index.entries import build_entries_from_instance


def test_build_entries_indexes_alias_terms_for_all_configured_views() -> None:
    """Metadata build does not filter alias-matching terms; self-ref is target-driven."""
    instance = {
        "externalId": "EQ-SELF",
        "properties": {
            "aliases": ["P-101A"],
            "name": "P-101A",
            "description": "See P-101A and P-102B on line L-200",
        },
    }
    for view_name in ("CogniteFile", "CogniteAsset", "CogniteEquipment", "CogniteTimeSeries"):
        view_cfg = next(v for v in INDEX_FIELD_CONFIG if v["view"] == view_name)
        entries, _instance_meta = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
        terms = {e["normalized_term"] for e in entries}
        assert "p101a" in terms
        assert "p102b" in terms
        assert entries[0]["reference_type"] == view_name


def test_build_entries_uses_instance_space_as_reference_space() -> None:
    view_cfg = next(v for v in INDEX_FIELD_CONFIG if v["view"] == "CogniteEquipment")
    instance = {
        "externalId": "EQ-1",
        "space": "springfield_instances",
        "properties": {
            "name": "P-102B",
            "description": "Pump",
        },
    }
    entries, _instance_meta = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
    assert entries
    assert entries[0]["reference_space"] == "springfield_instances"
    assert entries[0]["additional_metadata"]["instance_space"] == "springfield_instances"


def test_build_entries_cognitefile_same_property_config_as_equipment() -> None:
    file_cfg = next(v for v in INDEX_FIELD_CONFIG if v["view"] == "CogniteFile")
    equipment_cfg = next(v for v in INDEX_FIELD_CONFIG if v["view"] == "CogniteEquipment")
    assert file_cfg["properties"] == equipment_cfg["properties"]


def test_build_entries_extracts_file_metadata_from_description() -> None:
    view_cfg = next(v for v in INDEX_FIELD_CONFIG if v["view"] == "CogniteEquipment")
    instance = {
        "externalId": "EQ-1",
        "properties": {
            "name": "P-101A",
            "description": "See drawing PH-ME-P-0160-001.pdf for details",
        },
    }
    entries, _instance_meta = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
    file_entries = [e for e in entries if e["source_type"] == "file_metadata"]
    assert any(e["term"] == "PH-ME-P-0160-001.pdf" for e in file_entries)
    assert all(e["reference_type"] == "CogniteEquipment" for e in file_entries)


def test_build_entries_extracts_master_doc_ref_without_extension() -> None:
    view_cfg = next(v for v in INDEX_FIELD_CONFIG if v["view"] == "CogniteFile")
    instance = {
        "externalId": "VAL_rev.pdf",
        "properties": {
            "name": "PH-ME-P-0160-002.pdf",
            "description": "Supersedes master PH-ME-P-0160-001; prior revision PH-ME-P-0153-001",
        },
    }
    entries, _instance_meta = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
    file_terms = {
        e["term"]
        for e in entries
        if e["source_type"] == "file_metadata" and e["source_property"] == "description"
    }
    assert "PH-ME-P-0160-001" in file_terms
    assert "PH-ME-P-0153-001" in file_terms


def test_build_entries_scoped_merge_override() -> None:
    scope_config = {
        "enabled": True,
        "levels": ["site", "unit"],
        "scope_key_template": "site:{site}|unit:{unit}",
        "strict_scope": False,
        "fallback_scope_key": "global",
        "resolve_from": {
            "CogniteEquipment": {
                "site": ["sourceContext"],
                "unit": ["sourceId"],
            }
        },
        "resolve_from_default": {},
    }
    view_cfg = {
        "view": "CogniteEquipment",
        "properties": [
            {
                "path": "description",
                "source_type": "asset_metadata",
                "extract_pattern": r"\bDEFAULT-\d+\b",
            }
        ],
        "properties_by_scope": {
            "site:Rotterdam|unit:U100": {
                "mode": "merge",
                "properties": [
                    {
                        "path": "description",
                        "source_type": "asset_metadata",
                        "extract_pattern": r"\bEQ-\d{5}\b",
                    }
                ],
            }
        },
    }
    instance = {
        "externalId": "EQ-1",
        "properties": {
            "sourceContext": "Rotterdam",
            "sourceId": "U100",
            "description": "Pump EQ-12345 on line",
        },
    }
    entries, meta = build_entries_from_instance(instance, view_cfg, scope_config)
    assert meta["scope_override_resolution"] == "exact"
    assert any(e["term"] == "EQ-12345" for e in entries)


def test_build_entries_ambiguous_scope_override_skips() -> None:
    scope_config = {
        "enabled": True,
        "levels": ["site", "unit"],
        "scope_key_template": "site:{site}|unit:{unit}",
        "strict_scope": False,
        "fallback_scope_key": "global",
        "resolve_from": {
            "CogniteEquipment": {
                "site": ["sourceContext"],
                "unit": ["sourceId"],
            }
        },
        "resolve_from_default": {},
    }
    view_cfg = {
        "view": "CogniteEquipment",
        "properties": [{"path": "description", "source_type": "asset_metadata"}],
        "properties_by_scope": {
            "site:Rotterdam|unit:*": {
                "mode": "merge",
                "properties": [
                    {"path": "description", "source_type": "asset_metadata", "extract_pattern": r"\bA\b"}
                ],
            },
            "site:*|unit:U100": {
                "mode": "merge",
                "properties": [
                    {"path": "description", "source_type": "asset_metadata", "extract_pattern": r"\bB\b"}
                ],
            },
        },
    }
    instance = {
        "externalId": "EQ-1",
        "properties": {
            "sourceContext": "Rotterdam",
            "sourceId": "U100",
            "description": "Pump",
        },
    }
    entries, meta = build_entries_from_instance(instance, view_cfg, scope_config)
    assert entries == []
    assert meta["skip_reason"] == "scope_property_override_ambiguous"

