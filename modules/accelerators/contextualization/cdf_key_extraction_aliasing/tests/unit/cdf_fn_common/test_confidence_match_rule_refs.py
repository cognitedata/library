"""Tests for shared ``confidence_match_rule_definitions`` resolution."""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.cdf_fn_common.confidence_match_rule_refs import (  # noqa: E402
    definitions_lookup_from_scope,
    dedupe_confidence_match_rules_by_name,
    expand_confidence_match_rules_list,
    resolve_confidence_match_rule_refs_in_scope_document,
)


def test_definitions_map_and_list_form() -> None:
    doc_map = {
        "confidence_match_rule_definitions": {
            "r1": {"match": {"x": 1}, "priority": 1},
            "r2": {"name": "custom_name", "priority": 2},
        }
    }
    m = definitions_lookup_from_scope(doc_map)
    assert "r1" in m and m["r1"]["name"] == "r1"
    assert "custom_name" in m and m["custom_name"]["priority"] == 2

    doc_list = {
        "confidence_match_rule_definitions": [
            {"name": "from_list", "priority": 3},
        ]
    }
    m2 = definitions_lookup_from_scope(doc_list)
    assert m2["from_list"]["priority"] == 3


def test_expand_string_ref_and_inline() -> None:
    lookup = {
        "a": {"name": "a", "priority": 1},
    }
    out = expand_confidence_match_rules_list(
        ["a", {"name": "b", "priority": 5, "match": {}}],
        lookup,
        context="test.",
    )
    assert len(out) == 2
    assert out[0]["name"] == "a"
    assert out[1]["name"] == "b"


def test_expand_ref_with_overrides() -> None:
    lookup = {"base": {"name": "base", "priority": 1, "match": {"k": 1}}}
    out = expand_confidence_match_rules_list(
        [{"ref": "base", "priority": 99}],
        lookup,
    )
    assert out[0]["name"] == "base"
    assert out[0]["priority"] == 99
    assert out[0]["match"] == {"k": 1}


def test_missing_ref_raises() -> None:
    with pytest.raises(ValueError, match="unknown"):
        expand_confidence_match_rules_list(["nope"], {})


def test_expand_shorthand_chain_mapping() -> None:
    """``{ rule_id: [ tail ids... ] }`` normalizes to ordered hierarchy; refs expand in children."""
    lookup = {
        "b": {"name": "b", "priority": 1, "match": {"keywords": ["x"]}},
        "c": {"name": "c", "priority": 2, "match": {"keywords": ["y"]}},
    }
    out = expand_confidence_match_rules_list(
        [{"b": ["c"]}],
        lookup,
        context="t.",
    )
    assert len(out) == 1
    hi = out[0]["hierarchy"]
    assert hi["mode"] == "ordered"
    ch = hi["children"]
    assert len(ch) == 2
    assert ch[0]["name"] == "b"
    assert ch[1]["name"] == "c"


def test_expand_hierarchy_preserves_nesting() -> None:
    lookup = {
        "a": {"name": "a", "priority": 1, "match": {"keywords": ["x"]}},
        "b": {"name": "b", "priority": 2, "match": {"keywords": ["y"]}},
    }
    out = expand_confidence_match_rules_list(
        [{"hierarchy": {"mode": "concurrent", "children": ["a", "b"]}}],
        lookup,
        context="t.",
    )
    assert len(out) == 1
    assert "hierarchy" in out[0]
    ch = (out[0]["hierarchy"] or {}).get("children") or []
    assert [r["name"] for r in ch] == ["a", "b"]


def test_expand_named_sequence() -> None:
    lookup = {
        "a": {"name": "a", "priority": 1, "match": {"keywords": ["x"]}},
        "b": {"name": "b", "priority": 2, "match": {"keywords": ["y"]}},
    }
    seq = {"ab": ["a", "b"]}
    out = expand_confidence_match_rules_list(
        [{"sequence": "ab"}],
        lookup,
        sequences=seq,
        context="t.",
    )
    assert [r["name"] for r in out] == ["a", "b"]


def test_dedupe_by_name() -> None:
    rules = [
        {"name": "x", "priority": 1, "match": {"keywords": ["a"]}},
        {"name": "x", "priority": 99, "match": {"keywords": ["b"]}},
        {"name": "y", "match": {"keywords": ["c"]}},
    ]
    d = dedupe_confidence_match_rules_by_name(rules)
    assert len(d) == 2
    assert d[0]["priority"] == 1


def test_targets_merged_into_rules() -> None:
    doc = {
        "confidence_match_rule_definitions": {
            "r1": {"name": "r1", "priority": 1, "match": {"keywords": ["a"]}},
            "r2": {"name": "r2", "priority": 2, "match": {"keywords": ["b"]}},
        },
        "key_extraction": {
            "config": {
                "data": {
                    "validation": {
                        "confidence_match_rules": ["r1"],
                        "confidence_match_rule_targets": ["r2"],
                    }
                }
            }
        },
    }
    resolve_confidence_match_rule_refs_in_scope_document(doc)
    rules = doc["key_extraction"]["config"]["data"]["validation"]["confidence_match_rules"]
    assert [r["name"] for r in rules] == ["r1", "r2"]


def test_resolve_full_scope_nested_paths() -> None:
    doc = {
        "confidence_match_rule_definitions": {
            "vr": {"name": "vr", "priority": 1},
        },
        "source_views": [
            {
                "validation": {
                    "confidence_match_rules": ["vr"],
                }
            }
        ],
        "key_extraction": {
            "config": {
                "data": {
                    "validation": {"confidence_match_rules": ["vr"]},
                    "extraction_rules": [
                        {"validation": {"confidence_match_rules": [{"ref": "vr", "priority": 2}]}}
                    ],
                    "source_views": [
                        {"validation": {"confidence_match_rules": ["vr"]}},
                    ],
                }
            }
        },
        "aliasing": {
            "config": {
                "data": {
                    "validation": {"confidence_match_rules": ["vr"]},
                    "aliasing_rules": [
                        {"validation": {"confidence_match_rules": ["vr"]}},
                    ],
                }
            }
        },
    }
    orig = copy.deepcopy(doc)
    resolve_confidence_match_rule_refs_in_scope_document(doc)
    assert "confidence_match_rule_definitions" not in doc
    assert doc["source_views"][0]["validation"]["confidence_match_rules"][0]["priority"] == 1
    ke_data = doc["key_extraction"]["config"]["data"]
    assert ke_data["validation"]["confidence_match_rules"][0]["name"] == "vr"
    assert ke_data["extraction_rules"][0]["validation"]["confidence_match_rules"][0]["priority"] == 2
    assert ke_data["source_views"][0]["validation"]["confidence_match_rules"][0]["name"] == "vr"
    al_data = doc["aliasing"]["config"]["data"]
    assert al_data["validation"]["confidence_match_rules"][0]["name"] == "vr"
    assert al_data["aliasing_rules"][0]["validation"]["confidence_match_rules"][0]["name"] == "vr"
    # Original untouched (deepcopy check)
    assert "confidence_match_rule_definitions" in orig


def test_resolve_no_definitions_strips_key_only_when_present() -> None:
    doc: dict = {}
    resolve_confidence_match_rule_refs_in_scope_document(doc)
    assert doc == {}


def test_sequences_stripped_after_resolve() -> None:
    doc = {
        "confidence_match_rule_sequences": {"s": ["a"]},
        "confidence_match_rule_definitions": {
            "a": {"name": "a", "match": {"keywords": ["z"]}},
        },
        "key_extraction": {
            "config": {
                "data": {
                    "validation": {"confidence_match_rules": [{"sequence": "s"}]},
                }
            }
        },
    }
    resolve_confidence_match_rule_refs_in_scope_document(doc)
    assert "confidence_match_rule_sequences" not in doc
    rules = doc["key_extraction"]["config"]["data"]["validation"]["confidence_match_rules"]
    assert rules[0]["name"] == "a"

