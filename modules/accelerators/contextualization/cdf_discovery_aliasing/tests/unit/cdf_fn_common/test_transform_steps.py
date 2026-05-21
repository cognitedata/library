"""Tests for multi-step transform execution."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from fn_dm_transform.engine.transform_steps import apply_transform_steps_to_props  # noqa: E402


def test_ordered_steps_chain_output_field() -> None:
    props = {"name": "  HELLO  "}
    cfg = {
        "execution": {"mode": "ordered"},
        "steps": [
            {
                "handler_id": "trim_whitespace",
                "fields": [{"field_name": "name"}],
                "output_template": "{name}",
                "output_field": "indexKey",
                "trim_whitespace": {"mode": "ends_only"},
            },
            {
                "handler_id": "change_case",
                "fields": [{"field_name": "indexKey"}],
                "output_field": "indexKey",
                "output_mode": "overwrite",
                "change_case": {"mode": "lower"},
            },
        ],
    }
    out = apply_transform_steps_to_props(props, cfg)
    assert len(out) == 1
    assert out[0]["indexKey"] == "hello"


def test_parallel_steps_merge_list_aliases() -> None:
    props = {"name": "A", "description": "B"}
    cfg = {
        "execution": {"mode": "parallel"},
        "field_policies": [
            {
                "property": "aliases",
                "strategy": "merge_list",
                "merge_list": {"unique": True, "branch_order": "by_score"},
            }
        ],
        "steps": [
            {
                "handler_id": "trim_whitespace",
                "fields": [{"field_name": "name"}],
                "output_template": "{name}",
                "output_field": "aliases",
                "trim_whitespace": {},
            },
            {
                "handler_id": "trim_whitespace",
                "fields": [{"field_name": "description"}],
                "output_template": "{description}",
                "output_field": "aliases",
                "trim_whitespace": {},
            },
        ],
    }
    out = apply_transform_steps_to_props(props, cfg)
    assert len(out) == 1
    assert out[0]["aliases"] == ["A", "B"]
