"""Tests for join cohort property merge."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))

from cdf_fn_common.etl_join_orchestration import merge_join_props  # noqa: E402


def test_merge_join_props_flattens_raw_columns_with_prefix() -> None:
    left = {"name": "P-101"}
    right = {"raw_columns": {"scope": "global", "key": "P-101", "aliases": "EXP-P-101"}}
    merged = merge_join_props(left, right, "explicit_")
    assert merged["name"] == "P-101"
    assert merged["explicit_key"] == "P-101"
    assert merged["explicit_aliases"] == "EXP-P-101"
    assert "explicit_raw_columns" not in merged


def test_merge_join_props_keeps_raw_columns_without_prefix() -> None:
    left = {"name": "P-101"}
    right = {"raw_columns": {"key": "P-101", "aliases": "EXP-P-101"}}
    merged = merge_join_props(left, right, "")
    assert merged["raw_columns"]["aliases"] == "EXP-P-101"
