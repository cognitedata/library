"""Unit tests for per-alias confidence_filter stage."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.cohort_confidence_value_filter import (  # noqa: E402
    apply_confidence_value_filter,
    validate_confidence_filter_config,
)


def test_prune_aliases_by_threshold() -> None:
    cfg = {
        "description": "gate",
        "value_field": "aliases",
        "min_confidence": 0.5,
        "comparison": "gte",
    }
    out = apply_confidence_value_filter(
        {"aliases": ["a", "b", "c"], "aliases_confidence": [0.9, 0.4, 0.6]},
        cfg,
    )
    assert out is not None
    assert out["aliases"] == ["a", "c"]
    assert out["aliases_confidence"] == [pytest.approx(0.9), pytest.approx(0.6)]
    assert "confidence" not in out


def test_drop_row_when_empty() -> None:
    cfg = {
        "description": "gate",
        "value_field": "aliases",
        "min_confidence": 0.9,
        "comparison": "gte",
        "drop_row_if_empty": True,
    }
    out = apply_confidence_value_filter(
        {"aliases": ["x"], "aliases_confidence": [0.1]},
        cfg,
    )
    assert out is None


def test_strips_stale_score_keys() -> None:
    cfg = {"description": "gate", "value_field": "aliases", "min_confidence": 0.0}
    out = apply_confidence_value_filter(
        {
            "aliases": ["a"],
            "aliases_confidence": [0.5],
            "confidence": [0.1],
            "indexKey_confidence": [0.2],
        },
        cfg,
    )
    assert out is not None
    assert "confidence" not in out
    assert "indexKey_confidence" not in out


def test_validate_confidence_filter_config_requires_description() -> None:
    with pytest.raises(ValueError, match="description"):
        validate_confidence_filter_config({})
    validate_confidence_filter_config({"description": "ok"})


def test_compile_rejects_legacy_filter_kind() -> None:
    from cdf_fn_common.workflow_compile.canvas_dag import CanvasCompileError, compile_canvas_dag

    doc = {
        "compile_workflow_dag": "canvas",
        "canvas": {
            "nodes": [
                {"id": "tr", "kind": "transform", "data": {"config": {"description": "t0"}}},
                {"id": "fl", "kind": "filter", "data": {"config": {"description": "gate", "filters": []}}},
            ],
            "edges": [{"source": "tr", "target": "fl"}],
        },
    }
    with pytest.raises(CanvasCompileError, match="instance_filter"):
        compile_canvas_dag(doc)
