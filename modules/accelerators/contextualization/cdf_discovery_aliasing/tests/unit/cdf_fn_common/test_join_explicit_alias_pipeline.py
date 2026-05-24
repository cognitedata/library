"""Join merge + explicit-alias transform pipeline (EXP- prefixed aliases)."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
if str(_MODULE_ROOT / "functions") not in sys.path:
    sys.path.insert(0, str(_MODULE_ROOT / "functions"))

from fn_dm_join.engine.join_on_eval import eval_join_on  # noqa: E402
from fn_dm_join.engine.orchestration import _merge_props  # noqa: E402
from fn_dm_transform.engine.pipeline import transform_row_properties  # noqa: E402

_EXPLICIT_ALIAS_TRANSFORM_CFG = {
    "handler_id": "trim_whitespace",
    "enabled": True,
    "fields": [
        {
            "field_name": "explicit_aliases",
            "required": True,
            "priority": 1,
            "regex": "",
        }
    ],
    "output_field": "aliases",
    "output_template": "{explicit_aliases}",
    "output_mode": "append",
    "trim_whitespace": {"mode": "ends_only"},
}

_JOIN_ON_KEY_OR_NAME = {
    "or": [
        {
            "operator": "IEQUALS",
            "left_property": "name",
            "right_property": "raw_columns.key",
        },
        {
            "operator": "IEQUALS",
            "left_property": "name",
            "right_property": "raw_columns.name",
        },
    ]
}


def _aliases_from_joined_props(merged: dict) -> list:
    out = transform_row_properties(merged, _EXPLICIT_ALIAS_TRANSFORM_CFG)
    aliases = out[0].get("aliases")
    if isinstance(aliases, list):
        return aliases
    return [aliases] if aliases else []


def test_merge_and_transform_produces_exp_alias_from_key_column() -> None:
    left = {"name": "P-101"}
    right = {"raw_columns": {"scope": "global", "key": "P-101", "aliases": "EXP-P-101"}}
    assert eval_join_on(left, right, _JOIN_ON_KEY_OR_NAME)
    merged = _merge_props(left, right, "explicit_")
    assert merged["explicit_aliases"] == "EXP-P-101"
    assert _aliases_from_joined_props(merged) == ["EXP-P-101"]


def test_merge_and_transform_produces_exp_alias_from_name_column() -> None:
    left = {"name": "P-101"}
    right = {"raw_columns": {"scope": "global", "name": "P-101", "aliases": "EXP-P-101"}}
    assert eval_join_on(left, right, _JOIN_ON_KEY_OR_NAME)
    merged = _merge_props(left, right, "explicit_")
    assert merged["explicit_aliases"] == "EXP-P-101"
    assert _aliases_from_joined_props(merged) == ["EXP-P-101"]


def test_nested_explicit_raw_columns_does_not_feed_explicit_aliases_transform() -> None:
    """Without flattening ``raw_columns``, transform reads no ``explicit_aliases`` and skips EXP-."""
    props = {
        "name": "P-101",
        "explicit_raw_columns": {"key": "P-101", "aliases": "EXP-P-101"},
    }
    assert _aliases_from_joined_props(props) == []
