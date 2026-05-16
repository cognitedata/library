"""Unit tests for discovery validate stage."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.discovery_validate import (  # noqa: E402
    materialize_validation_rules,
    validate_row_properties,
    validate_validation_config,
)


def _blacklist_rule() -> dict:
    return {
        "name": "blacklist",
        "enabled": True,
        "priority": 10,
        "match": {"keywords": ["test"], "expressions": []},
        "confidence_modifier": {"mode": "explicit", "value": 0.0},
    }


def test_materialize_rules_from_definitions_map() -> None:
    cfg = {
        "description": "blk",
        "validation_rule_definitions": {"blacklist": _blacklist_rule()},
    }
    rules = materialize_validation_rules(cfg)
    assert len(rules) == 1
    assert rules[0]["name"] == "blacklist"


def test_validate_row_blacklist_zeros_confidence() -> None:
    cfg = {
        "description": "blk",
        "validation_rule_definitions": {"blacklist": _blacklist_rule()},
        "validate_fields": ["aliases"],
        "min_confidence": 0.0,
    }
    rules = materialize_validation_rules(cfg)
    out = validate_row_properties(
        {"aliases": ["good", "test-tag"]},
        cfg,
        rules,
    )
    assert out["aliases"] == ["good", "test-tag"]
    assert out["confidence"][0] == pytest.approx(1.0)
    assert out["confidence"][1] == pytest.approx(0.0)


def test_validate_row_explicit_stops_chain() -> None:
    cfg = {
        "description": "isa",
        "validation_rule_definitions": {
            "isa": {
                "name": "isa",
                "match": {"expressions": [r"^P-\d+$"], "keywords": []},
                "confidence_modifier": {"mode": "explicit", "value": 1.0},
            },
            "penalty": {
                "name": "penalty",
                "match": {"expressions": [".*"], "keywords": []},
                "confidence_modifier": {"mode": "offset", "value": -0.5},
            },
        },
        "validate_fields": ["aliases"],
    }
    rules = materialize_validation_rules(cfg)
    out = validate_row_properties({"aliases": ["P-101"]}, cfg, rules)
    assert out["confidence"][0] == pytest.approx(1.0)


def test_validate_row_reads_legacy_aliases_confidence() -> None:
    cfg = {
        "description": "blk",
        "validation_rule_definitions": {"blacklist": _blacklist_rule()},
        "validate_fields": ["aliases"],
        "min_confidence": 0.0,
    }
    rules = materialize_validation_rules(cfg)
    out = validate_row_properties(
        {"aliases": ["good", "test-tag"], "aliases_confidence": [0.5, 0.6]},
        cfg,
        rules,
    )
    assert out["aliases"] == ["good", "test-tag"]
    assert out["confidence"][0] == pytest.approx(0.5)
    assert out["confidence"][1] == pytest.approx(0.0)
    assert "aliases_confidence" not in out


    cfg = {
        "description": "thresh",
        "validation_rule_definitions": {"blacklist": _blacklist_rule()},
        "validate_fields": ["aliases"],
        "min_confidence": 0.5,
    }
    rules = materialize_validation_rules(cfg)
    out = validate_row_properties({"aliases": ["ok", "test"]}, cfg, rules)
    assert out["aliases"] == ["ok"]


def test_discovered_key_writes_top_level_confidence() -> None:
    cfg = {
        "description": "blk",
        "validation_rule_definitions": {"blacklist": _blacklist_rule()},
        "validate_fields": ["discoveredKey"],
        "min_confidence": 0.0,
    }
    rules = materialize_validation_rules(cfg)
    out = validate_row_properties({"discoveredKey": ["good", "test-tag"]}, cfg, rules)
    assert out["discoveredKey"] == ["good", "test-tag"]
    assert "confidence" in out
    assert "discoveredKey_confidence" not in out
    assert out["confidence"][0] == pytest.approx(1.0)
    assert out["confidence"][1] == pytest.approx(0.0)


def test_reads_top_level_confidence_for_discovered_key() -> None:
    cfg = {
        "description": "blk",
        "validation_rule_definitions": {"blacklist": _blacklist_rule()},
        "validate_fields": ["discoveredKey"],
        "min_confidence": 0.0,
    }
    rules = materialize_validation_rules(cfg)
    out = validate_row_properties(
        {"discoveredKey": ["good", "test-tag"], "confidence": [0.2, 0.8]},
        cfg,
        rules,
    )
    assert out["confidence"][0] == pytest.approx(0.2)
    assert out["confidence"][1] == pytest.approx(0.0)


def test_scored_objects_discovered_key_drops_parallel_confidence_keys() -> None:
    cfg = {
        "description": "blk",
        "validation_rule_definitions": {"blacklist": _blacklist_rule()},
        "validate_fields": ["discoveredKey"],
        "output_mode": "scored_objects",
        "min_confidence": 0.0,
    }
    rules = materialize_validation_rules(cfg)
    out = validate_row_properties(
        {
            "discoveredKey": ["a", "test-x"],
            "confidence": [0.5, 0.6],
        },
        cfg,
        rules,
    )
    assert "confidence" not in out
    assert "discoveredKey_confidence" not in out
    assert out["discoveredKey"][0]["value"] == "a"
    assert out["discoveredKey"][0]["confidence"] == pytest.approx(0.5)
    assert out["discoveredKey"][1]["value"] == "test-x"
    assert out["discoveredKey"][1]["confidence"] == pytest.approx(0.0)


def test_validate_config_requires_description() -> None:
    with pytest.raises(ValueError, match="description"):
        validate_validation_config({})
