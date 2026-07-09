"""Unit tests for virtual tag creation (UC4)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from inverted_index.config import VIRTUAL_TAG_CREATION_CONFIG
from inverted_index.storage.memory_adapter import MemoryStorageAdapter
from inverted_index.virtual_tags import (
    build_structural_assets,
    build_virtual_tag_asset,
    is_missing_tag_term,
    process_virtual_tags_for_index_entries,
    term_passes_selection_mode,
    upsert_virtual_tags_for_terms,
)


def _scope_config() -> dict:
    return {
        "enabled": True,
        "levels": ["site", "unit"],
        "scope_key_template": "site:{site}|unit:{unit}",
        "fallback_scope_key": "global",
    }


def _virtual_tag_config(**overrides) -> dict:
    cfg = {
        **VIRTUAL_TAG_CREATION_CONFIG,
        "enabled": True,
        "instance_space": "inst_virtual_tags",
        "term_selection_mode": "all",
    }
    cfg.update(overrides)
    return cfg


def test_build_structural_assets_hierarchy() -> None:
    scope_values = {"site": "Rotterdam", "unit": "U100"}
    assets, deepest = build_structural_assets(
        scope_values,
        ["site", "unit"],
        _virtual_tag_config(),
    )
    assert deepest == "unit_Rotterdam_U100"
    assert len(assets) == 2
    assert assets[0]["externalId"] == "site_Rotterdam"
    assert assets[1]["properties"]["parent"]["externalId"] == "site_Rotterdam"


def test_build_virtual_tag_asset_leaf() -> None:
    scope_values = {"site": "Rotterdam", "unit": "U100"}
    hits = [
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "source_type": "diagram_annotation_pattern",
            "reference_external_id": "FILE_1",
        }
    ]
    asset = build_virtual_tag_asset(
        scope_values,
        "P-101A",
        hits,
        _virtual_tag_config(),
        hierarchy_levels=["site", "unit"],
        parent_external_id="unit_Rotterdam_U100",
    )
    assert asset["externalId"].startswith("asset_tag_")
    assert asset["properties"]["name"] == "P-101A"
    assert asset["properties"]["aliases"] == ["P-101A"]
    assert asset["properties"]["sourceContext"] == "Rotterdam"
    assert asset["properties"]["sourceId"] == "U100"
    assert asset["properties"]["parent"]["externalId"] == "unit_Rotterdam_U100"


def test_upsert_virtual_tags_dry_run() -> None:
    hits = {
        "p101a": [
            {
                "term": "P-101A",
                "normalized_term": "p101a",
                "source_type": "diagram_annotation_pattern",
            }
        ]
    }
    result = upsert_virtual_tags_for_terms(
        None,
        "site:Rotterdam|unit:U100",
        hits,
        virtual_tag_config=_virtual_tag_config(),
        scope_config=_scope_config(),
        dry_run=True,
    )
    assert result["leaf_assets"] == 1
    assert result["structural_assets"] == 2
    assert result["dry_run"] is True


@patch("inverted_index.virtual_tags.cognite_asset_exists_for_term", return_value=False)
def test_missing_tag_term_requires_pattern(_mock_exists: MagicMock) -> None:
    hits = [
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "source_type": "diagram_annotation_pattern",
        }
    ]
    assert is_missing_tag_term(
        MagicMock(),
        "site:Rotterdam|unit:U100",
        "p101a",
        hits,
        virtual_tag_config=_virtual_tag_config(term_selection_mode="missing_tags_only"),
        scope_config=_scope_config(),
    )


@patch("inverted_index.virtual_tags.cognite_asset_exists_for_term", return_value=False)
def test_missing_tag_rejects_metadata_only(_mock_exists: MagicMock) -> None:
    hits = [
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "source_type": "asset_metadata",
            "reference_type": "CogniteEquipment",
        }
    ]
    assert not is_missing_tag_term(
        MagicMock(),
        "site:Rotterdam|unit:U100",
        "p101a",
        hits,
        virtual_tag_config=_virtual_tag_config(term_selection_mode="missing_tags_only"),
        scope_config=_scope_config(),
    )


@patch("inverted_index.virtual_tags.cognite_asset_exists_for_term", return_value=True)
def test_missing_tag_skips_existing_asset(_mock_exists: MagicMock) -> None:
    hits = [
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "source_type": "diagram_annotation_pattern",
        }
    ]
    assert not is_missing_tag_term(
        MagicMock(),
        "site:Rotterdam|unit:U100",
        "p101a",
        hits,
        virtual_tag_config=_virtual_tag_config(term_selection_mode="missing_tags_only"),
        scope_config=_scope_config(),
    )


def test_term_passes_selection_mode_all() -> None:
    hits = [{"source_type": "asset_metadata", "term": "X", "normalized_term": "x"}]
    assert term_passes_selection_mode(
        None,
        "site:A|unit:1",
        "x",
        hits,
        virtual_tag_config=_virtual_tag_config(term_selection_mode="all"),
        scope_config=_scope_config(),
        term_selection_mode="all",
    )


@patch("inverted_index.virtual_tags.apply_virtual_assets")
@patch("inverted_index.virtual_tags.load_term_hits")
@patch("inverted_index.virtual_tags.term_passes_selection_mode", return_value=True)
def test_process_virtual_tags_incremental(
    _passes: MagicMock,
    mock_load: MagicMock,
    mock_apply: MagicMock,
) -> None:
    mock_load.return_value = [
        {
            "term": "P-101A",
            "normalized_term": "p101a",
            "source_type": "diagram_annotation_pattern",
        }
    ]
    mock_apply.return_value = {"assets_applied": 3, "dry_run": False}
    adapter = MemoryStorageAdapter()
    entries = [
        {
            "match_scope_key": "site:Rotterdam|unit:U100",
            "normalized_term": "p101a",
            "term": "P-101A",
            "source_type": "diagram_annotation_pattern",
        }
    ]
    result = process_virtual_tags_for_index_entries(
        MagicMock(),
        entries,
        virtual_tag_config=_virtual_tag_config(),
        scope_config=_scope_config(),
        storage_adapter=adapter,
        dry_run=False,
    )
    assert result["terms_processed"] == 1
    assert result["leaf_assets"] == 1
