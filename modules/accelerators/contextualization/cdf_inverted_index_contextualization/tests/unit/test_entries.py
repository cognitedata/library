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
        entries = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
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
    entries = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
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
    entries = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
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
    entries = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
    file_terms = {
        e["term"]
        for e in entries
        if e["source_type"] == "file_metadata" and e["source_property"] == "description"
    }
    assert "PH-ME-P-0160-001" in file_terms
    assert "PH-ME-P-0153-001" in file_terms

