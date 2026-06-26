"""Unit tests for index entry build self-reference filtering."""

from inverted_index.config import INDEX_FIELD_CONFIG, SCOPE_CONFIG
from inverted_index.entries import build_entries_from_instance


def test_build_entries_skips_terms_matching_instance_aliases() -> None:
    view_cfg = next(v for v in INDEX_FIELD_CONFIG if v["view"] == "CogniteEquipment")
    instance = {
        "externalId": "EQ-SELF",
        "properties": {
            "aliases": ["P-101A"],
            "name": "P-101A",
            "description": "See P-101A and P-102B on line L-200",
        },
    }
    entries = build_entries_from_instance(instance, view_cfg, SCOPE_CONFIG)
    terms = {e["normalized_term"] for e in entries}
    assert "p101a" not in terms
    assert "p102b" in terms
