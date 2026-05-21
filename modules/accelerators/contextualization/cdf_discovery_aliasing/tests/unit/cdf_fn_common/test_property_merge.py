"""Tests for shared property merge helpers."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.property_merge import (  # noqa: E402
    FieldPolicy,
    MergeListOptions,
    build_merged_props_for_instance,
    merge_property_dicts,
    parse_field_policies_from_list,
)


def test_merge_property_dicts_merge_list() -> None:
    policies = parse_field_policies_from_list(
        [{"property": "aliases", "strategy": "merge_list", "merge_list": {"unique": True}}]
    )
    merged = merge_property_dicts(
        [{"aliases": "a"}, {"aliases": "b"}],
        policies,
    )
    assert merged["aliases"] == ["a", "b"]


def test_build_merged_props_tie_break() -> None:
    policy = {
        "x": FieldPolicy("x", "tie_break", MergeListOptions()),
    }
    rows = [
        ((1.0, "r1", 0), 0, {"x": "low"}),
        ((2.0, "r2", 1), 1, {"x": "high"}),
    ]
    out = build_merged_props_for_instance(rows, policy)
    assert out["x"] == "high"
