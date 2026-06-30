"""Unit tests for match scope resolution."""

from inverted_index.config import SCOPE_CONFIG
from inverted_index.scope import (
    build_scope_key,
    normalize_resolve_from,
    parse_scope_key,
    resolve_match_scope,
    resolve_scope_level,
    slugify_scope_code,
)


def test_resolve_scope_level_fallback() -> None:
    instance = {"sourceContext": "", "siteId": "RTM", "parentSite": "OLD"}
    value, path = resolve_scope_level(
        instance, ["sourceContext", "siteId", "parentSite"]
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
                "site": ["sourceContext"],
                "unit": ["sourceId"],
            }
        },
        "strict_scope": True,
    }
    instance = {"properties": {"sourceContext": "Rotterdam", "sourceId": "U100"}}
    key, scope = resolve_match_scope(instance, "CogniteAsset", cfg)
    assert key == "site:Rotterdam|unit:U100"
    assert scope == {"site": "Rotterdam", "unit": "U100"}


def test_normalize_resolve_from_positional() -> None:
    result = normalize_resolve_from(["siteId", "unitId"], ["site", "unit"])
    assert result == {
        "site": [{"path": "siteId", "extract_mode": "passthrough"}],
        "unit": [{"path": "unitId", "extract_mode": "passthrough"}],
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
            "sourceId",
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


def test_parse_scope_key_round_trip() -> None:
    cfg = {
        "levels": ["site", "unit"],
        "scope_key_template": "site:{site}|unit:{unit}",
        "fallback_scope_key": "global",
    }
    parsed = parse_scope_key("site:Rotterdam|unit:U100", cfg)
    assert parsed == {"site": "Rotterdam", "unit": "U100"}
    assert build_scope_key(parsed, cfg) == "site:Rotterdam|unit:U100"


def test_parse_scope_key_global_returns_none() -> None:
    cfg = {"levels": ["site"], "fallback_scope_key": "global"}
    assert parse_scope_key("global", cfg) is None


def test_parse_scope_key_malformed() -> None:
    cfg = {
        "levels": ["site", "unit"],
        "scope_key_template": "site:{site}|unit:{unit}",
    }
    assert parse_scope_key("site:Only", cfg) is None
    assert parse_scope_key("", cfg) is None


def test_slugify_scope_code() -> None:
    assert slugify_scope_code("Rotterdam") == "Rotterdam"
    assert slugify_scope_code("P-101/A") == "P-101_A"
