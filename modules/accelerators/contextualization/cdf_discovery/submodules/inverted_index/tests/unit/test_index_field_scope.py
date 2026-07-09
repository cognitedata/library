"""Unit tests for scoped index field property overrides."""

from inverted_index.index_field_scope import (
    collect_view_config_property_paths,
    normalize_scope_properties_entry,
    resolve_scope_property_override,
    scope_pattern_matches_instance,
)

SCOPE_CONFIG = {
    "enabled": True,
    "levels": ["site", "unit"],
    "scope_key_template": "site:{site}|unit:{unit}",
    "strict_scope": False,
    "fallback_scope_key": "global",
    "resolve_from": {},
    "resolve_from_default": {},
}


def _view_cfg(**overrides) -> dict:
    base = {
        "view": "CogniteEquipment",
        "properties": [
            {
                "path": "description",
                "source_type": "asset_metadata",
                "extract_mode": "regex",
                "extract_pattern": r"\bDEFAULT-\d+\b",
            }
        ],
    }
    base.update(overrides)
    return base


def test_no_properties_by_scope_uses_default() -> None:
    result = resolve_scope_property_override(
        _view_cfg(),
        "site:Rotterdam|unit:U100",
        SCOPE_CONFIG,
    )
    assert result.resolution == "default"
    assert len(result.properties) == 1
    assert result.properties[0]["extract_pattern"] == r"\bDEFAULT-\d+\b"


def test_exact_scope_key_match() -> None:
    view_cfg = _view_cfg(
        properties_by_scope={
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
        }
    )
    result = resolve_scope_property_override(
        view_cfg, "site:Rotterdam|unit:U100", SCOPE_CONFIG
    )
    assert result.resolution == "exact"
    assert result.matched_key == "site:Rotterdam|unit:U100"
    assert result.properties[0]["extract_pattern"] == r"\bEQ-\d{5}\b"


def test_wildcard_tier_match_when_exact_missing() -> None:
    view_cfg = _view_cfg(
        properties_by_scope={
            "site:Rotterdam|unit:*": {
                "mode": "merge",
                "properties": [
                    {
                        "path": "description",
                        "source_type": "asset_metadata",
                        "extract_pattern": r"\bROT-\d{4}\b",
                    }
                ],
            }
        }
    )
    result = resolve_scope_property_override(
        view_cfg, "site:Rotterdam|unit:U100", SCOPE_CONFIG
    )
    assert result.resolution == "wildcard"
    assert result.matched_key == "site:Rotterdam|unit:*"
    assert result.properties[0]["extract_pattern"] == r"\bROT-\d{4}\b"


def test_replace_mode_ignores_defaults() -> None:
    view_cfg = _view_cfg(
        properties_by_scope={
            "site:Houston|unit:U200": {
                "mode": "replace",
                "properties": [
                    {
                        "path": "name",
                        "source_type": "asset_metadata",
                        "extract_pattern": r"\bHTX-\d+\b",
                    }
                ],
            }
        }
    )
    result = resolve_scope_property_override(
        view_cfg, "site:Houston|unit:U200", SCOPE_CONFIG
    )
    assert result.mode == "replace"
    assert len(result.properties) == 1
    assert result.properties[0]["path"] == "name"


def test_ambiguous_wildcard_patterns_abort() -> None:
    view_cfg = _view_cfg(
        properties_by_scope={
            "site:Rotterdam|unit:*": {
                "mode": "merge",
                "properties": [
                    {
                        "path": "description",
                        "source_type": "asset_metadata",
                        "extract_pattern": r"\bA-\d+\b",
                    }
                ],
            },
            "site:*|unit:U100": {
                "mode": "merge",
                "properties": [
                    {
                        "path": "description",
                        "source_type": "asset_metadata",
                        "extract_pattern": r"\bB-\d+\b",
                    }
                ],
            },
        }
    )
    result = resolve_scope_property_override(
        view_cfg, "site:Rotterdam|unit:U100", SCOPE_CONFIG
    )
    assert result.resolution == "ambiguous"
    assert result.properties == []
    assert len(result.matching_keys) == 2


def test_list_shorthand_treated_as_merge() -> None:
    entry = normalize_scope_properties_entry(
        [{"path": "description", "source_type": "asset_metadata"}]
    )
    assert entry["mode"] == "merge"
    assert len(entry["properties"]) == 1


def test_scope_pattern_matches_instance() -> None:
    assert scope_pattern_matches_instance(
        "site:Rotterdam|unit:*",
        "site:Rotterdam|unit:U100",
        SCOPE_CONFIG,
    )
    assert not scope_pattern_matches_instance(
        "site:Houston|unit:*",
        "site:Rotterdam|unit:U100",
        SCOPE_CONFIG,
    )


def test_collect_view_config_property_paths_includes_scoped_only() -> None:
    paths = collect_view_config_property_paths(
        {
            "properties": [{"path": "description", "source_type": "asset_metadata"}],
            "properties_by_scope": {
                "site:*": {
                    "mode": "merge",
                    "properties": [{"path": "metadata.notes", "source_type": "asset_metadata"}],
                }
            },
        }
    )
    assert "description" in paths
    assert "metadata.notes" in paths
