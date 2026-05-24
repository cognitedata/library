"""Tests for join cohort property merge (``_merge_props``)."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
if str(_MODULE_ROOT / "functions") not in sys.path:
    sys.path.insert(0, str(_MODULE_ROOT / "functions"))

from fn_dm_join.engine.orchestration import _merge_props  # noqa: E402


def test_merge_props_flattens_raw_columns_with_prefix() -> None:
    left = {"name": "P-101"}
    right = {
        "raw_columns": {
            "scope": "global",
            "key": "P-101",
            "aliases": "EXP-P-101",
        }
    }
    merged = _merge_props(left, right, "explicit_")
    assert merged["name"] == "P-101"
    assert merged["explicit_key"] == "P-101"
    assert merged["explicit_aliases"] == "EXP-P-101"
    assert "explicit_raw_columns" not in merged


def test_merge_props_keeps_raw_columns_without_prefix() -> None:
    left = {"name": "P-101"}
    right = {"raw_columns": {"key": "P-101", "aliases": "EXP-P-101"}}
    merged = _merge_props(left, right, "")
    assert merged["raw_columns"]["aliases"] == "EXP-P-101"


def test_merge_props_prefixes_top_level_right_keys() -> None:
    left = {"name": "P-101"}
    right = {"aliases": "EXP-P-101"}
    merged = _merge_props(left, right, "explicit_")
    assert merged["explicit_aliases"] == "EXP-P-101"
