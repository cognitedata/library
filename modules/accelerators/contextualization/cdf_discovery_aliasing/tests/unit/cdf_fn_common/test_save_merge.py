"""Tests for save_merge fan-in helpers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.save_merge import (  # noqa: E402
    build_merged_props_for_instance,
    merge_list_property_value,
    parse_field_policies,
    validate_save_config,
)


def test_validate_rejects_legacy_write_back() -> None:
    with pytest.raises(ValueError, match="no longer supports"):
        validate_save_config({"save_fan_in_mode": "none", "write_back_fields": ["x"]}, save_kind="view")


def test_validate_metadata_merge_list_classic() -> None:
    cfg = {
        "save_fan_in_mode": "none",
        "save_field_policies": [
            {"property": "metadata", "strategy": "merge_list", "merge_list": {}},
        ],
    }
    with pytest.raises(ValueError, match="metadata cannot use merge_list"):
        validate_save_config(cfg, save_kind="classic")


def test_merge_list_comma_join() -> None:
    rows = [{"aliases": ["a", "b"]}, {"aliases": "c"}]
    out = merge_list_property_value(rows, "aliases", unique=False)
    assert out == "a,b,c"


def test_merge_list_unique_segments() -> None:
    rows = [{"x": ["1"]}, {"x": ["1"]}, {"x": "2"}]
    out = merge_list_property_value(rows, "x", unique=True)
    assert out == "1,2"


def test_build_merged_tie_break_and_merge_list() -> None:
    pol = parse_field_policies(
        {
            "save_field_policies": [
                {"property": "aliases", "strategy": "merge_list", "merge_list": {"unique": False}},
            ]
        }
    )
    winner_props = {"name": "N1", "aliases": ["x"]}
    loser_props = {"name": "N2", "aliases": ["y"]}
    scored = [
        ((2.0, "r2", 1), 1, loser_props),
        ((3.0, "r1", 0), 0, winner_props),
    ]
    merged = build_merged_props_for_instance(scored, pol)
    assert merged["name"] == "N1"
    assert merged["aliases"] == "x,y"
