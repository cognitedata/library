"""Unit tests for match scope resolution."""

from inverted_index.config import SCOPE_CONFIG
from inverted_index.scope import (
    build_scope_key,
    normalize_resolve_from,
    resolve_match_scope,
    resolve_scope_level,
)


def test_resolve_scope_level_fallback() -> None:
    instance = {"metadata": {"site": ""}, "siteId": "RTM", "parentSite": "OLD"}
    value, path = resolve_scope_level(
        instance, ["metadata.site", "siteId", "parentSite"]
    )
    assert value == "RTM"
    assert path == "siteId"


def test_resolve_match_scope_disabled_uses_global() -> None:
    instance = {"properties": {"aliases": ["P-101A"]}}
    key, scope = resolve_match_scope(instance, "CogniteAsset", SCOPE_CONFIG)
    assert key == "global"
    assert scope == {}


def test_resolve_match_scope_when_enabled() -> None:
    cfg = {
        "enabled": True,
        "levels": ["site", "unit"],
        "scope_key_template": "site:{site}|unit:{unit}",
        "resolve_from": {
            "CogniteAsset": {
                "site": ["metadata.site"],
                "unit": ["metadata.unit"],
            }
        },
        "strict_scope": True,
    }
    instance = {"properties": {"metadata": {"site": "Rotterdam", "unit": "U100"}}}
    key, scope = resolve_match_scope(instance, "CogniteAsset", cfg)
    assert key == "site:Rotterdam|unit:U100"
    assert scope == {"site": "Rotterdam", "unit": "U100"}


def test_normalize_resolve_from_positional() -> None:
    result = normalize_resolve_from(["siteId", "unitId"], ["site", "unit"])
    assert result == {
        "site": [{"path": "siteId", "extract_mode": "whole_value"}],
        "unit": [{"path": "unitId", "extract_mode": "whole_value"}],
    }


def test_resolve_scope_level_regex() -> None:
    instance = {"description": "Plant site: Rotterdam unit: U100"}
    value, path = resolve_scope_level(
        instance,
        [
            {
                "path": "description",
                "extract_mode": "regex",
                "extract_pattern": r"site:\s*(\w+)",
            }
        ],
    )
    assert value == "Rotterdam"
    assert path == "description"


def test_resolve_scope_level_skips_alias_regex_match() -> None:
    instance = {
        "properties": {"aliases": ["P-101A"]},
        "description": "Tag P-101A on unit U100",
    }
    value, path = resolve_scope_level(
        instance,
        [
            {
                "path": "description",
                "extract_mode": "regex",
                "extract_pattern": r"\b[A-Z]{1,2}-\d{3,4}[A-Z]?\b",
            },
            "metadata.unit",
        ],
    )
    assert value is None
    assert path is None


def test_build_scope_key() -> None:
    key = build_scope_key(
        {"site": "Rotterdam", "unit": "U100"},
        {"scope_key_template": "site:{site}|unit:{unit}"},
    )
    assert key == "site:Rotterdam|unit:U100"
