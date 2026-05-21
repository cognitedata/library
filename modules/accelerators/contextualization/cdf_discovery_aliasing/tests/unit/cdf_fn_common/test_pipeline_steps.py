"""Tests for shared pipeline steps config."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.pipeline_steps import (  # noqa: E402
    EXECUTION_PARALLEL,
    materialize_transform_steps,
    materialize_validation_steps,
    validation_steps_to_rules_raw,
)


def test_materialize_transform_single_handler_legacy() -> None:
    cfg = {"handler_id": "trim_whitespace", "description": "t", "output_field": "x"}
    mode, steps = materialize_transform_steps(cfg)
    assert mode == "ordered"
    assert len(steps) == 1
    assert steps[0]["handler_id"] == "trim_whitespace"


def test_materialize_transform_explicit_steps() -> None:
    cfg = {
        "execution": {"mode": "parallel"},
        "steps": [
            {"handler_id": "trim_whitespace", "output_field": "a"},
            {"handler_id": "regex_substitution", "output_field": "b", "regex_substitution": {}},
        ],
    }
    mode, steps = materialize_transform_steps(cfg)
    assert mode == EXECUTION_PARALLEL
    assert len(steps) == 2


def test_materialize_validation_migrates_definitions_and_rules() -> None:
    cfg = {
        "validation_rule_definitions": {
            "r1": {"name": "r1", "match": {"expressions": ["x"]}},
        },
        "validation_rules": [{"name": "r2", "match": {"expressions": ["y"]}}],
    }
    mode, steps = materialize_validation_steps(cfg)
    assert mode == "ordered"
    assert len(steps) == 2
    names = {s.get("name") for s in steps}
    assert names == {"r1", "r2"}


def test_validation_parallel_compiles_to_hierarchy() -> None:
    steps = [{"name": "a"}, {"name": "b"}]
    raw = validation_steps_to_rules_raw(steps, EXECUTION_PARALLEL)
    assert len(raw) == 1
    h = raw[0].get("hierarchy")
    assert isinstance(h, dict)
    assert h.get("mode") == "concurrent"
