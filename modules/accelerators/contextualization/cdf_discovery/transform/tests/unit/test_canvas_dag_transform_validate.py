from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.workflow_compile.canvas_dag import CanvasCompileError, compile_canvas_dag  # noqa: E402


def _canvas_with_transform(config: dict) -> dict:
    return {
        "nodes": [
            {"id": "start", "kind": "start"},
            {
                "id": "t1",
                "kind": "transform",
                "data": {"config": config},
            },
            {"id": "end", "kind": "end"},
        ],
        "edges": [
            {"source": "start", "target": "t1"},
            {"source": "t1", "target": "end"},
        ],
    }


def test_transform_requires_output_field() -> None:
    canvas = _canvas_with_transform(
        {
            "handler_id": "trim_whitespace",
            "trim_whitespace": {"mode": "ends_only"},
            "fields": [{"field_name": "name"}],
        }
    )
    with pytest.raises(CanvasCompileError, match="output_field is required"):
        compile_canvas_dag(canvas)


def test_transform_requires_at_least_one_source_field() -> None:
    canvas = _canvas_with_transform(
        {
            "handler_id": "trim_whitespace",
            "trim_whitespace": {"mode": "ends_only"},
            "output_field": "aliases",
            "fields": [],
        }
    )
    with pytest.raises(CanvasCompileError, match="field_name is required"):
        compile_canvas_dag(canvas)


def test_transform_multi_step_validates_each_step() -> None:
    canvas = _canvas_with_transform(
        {
            "execution": {"mode": "ordered"},
            "steps": [
                {
                    "handler_id": "trim_whitespace",
                    "trim_whitespace": {"mode": "ends_only"},
                    "output_field": "draft",
                    "fields": [{"field_name": "name"}],
                },
                {
                    "handler_id": "trim_whitespace",
                    "trim_whitespace": {"mode": "ends_only"},
                    "output_field": "aliases",
                    "fields": [],
                },
            ],
        }
    )
    with pytest.raises(CanvasCompileError, match=r"step\[1\]"):
        compile_canvas_dag(canvas)


def test_transform_valid_config_compiles() -> None:
    canvas = _canvas_with_transform(
        {
            "handler_id": "trim_whitespace",
            "trim_whitespace": {"mode": "ends_only"},
            "output_field": "aliases",
            "fields": [{"field_name": "name"}],
        }
    )
    compiled = compile_canvas_dag(canvas)
    assert any(t["id"] == "t1" for t in compiled["tasks"])


def test_transform_unknown_handler_fails() -> None:
    canvas = _canvas_with_transform(
        {
            "handler_id": "not_a_real_handler",
            "output_field": "out",
            "fields": [{"field_name": "name"}],
        }
    )
    with pytest.raises(CanvasCompileError, match="handler_id must be one of"):
        compile_canvas_dag(canvas)


def test_transform_heuristic_sampler_requires_samples_or_pattern() -> None:
    canvas = _canvas_with_transform(
        {
            "handler_id": "heuristic_sampler",
            "heuristic_sampler": {"on_no_match": "keep_working"},
            "output_field": "aliases",
            "fields": [{"field_name": "name"}],
        }
    )
    with pytest.raises(CanvasCompileError, match="pattern.*samples"):
        compile_canvas_dag(canvas)


def test_transform_split_join_requires_template_or_indexes() -> None:
    canvas = _canvas_with_transform(
        {
            "handler_id": "split_join",
            "split_join": {"delimiter": "-"},
            "output_field": "tag",
            "fields": [{"field_name": "name"}],
        }
    )
    with pytest.raises(CanvasCompileError, match="split_join"):
        compile_canvas_dag(canvas)
