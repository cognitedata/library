from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: E402
    CanvasCompileError,
    compile_canvas_dag,
    validate_canvas_dag,
)


def _canvas_with_transform(config: dict, *, with_query: bool = True) -> dict:
    nodes = [{"id": "start", "kind": "start"}]
    edges = []
    upstream = "start"
    if with_query:
        nodes.append(
            {
                "id": "q1",
                "kind": "query_view",
                "data": {"config": {"view_external_id": "Asset"}},
            }
        )
        edges.append({"source": "start", "target": "q1"})
        upstream = "q1"
    nodes.extend(
        [
            {
                "id": "t1",
                "kind": "transform",
                "data": {"config": config},
            },
            {"id": "end", "kind": "end"},
        ]
    )
    edges.extend(
        [
            {"source": upstream, "target": "t1"},
            {"source": "t1", "target": "end"},
        ]
    )
    return {"nodes": nodes, "edges": edges}


def test_transform_requires_cohort_predecessor() -> None:
    canvas = _canvas_with_transform(
        {
            "handler_id": "trim_whitespace",
            "trim_whitespace": {"mode": "ends_only"},
            "output_field": "aliases",
            "fields": [{"field_name": "name"}],
        },
        with_query=False,
    )
    with pytest.raises(CanvasCompileError, match="materializes cohort rows"):
        compile_canvas_dag(canvas)


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


def test_query_raw_requires_source_db_and_table() -> None:
    canvas = {
        "nodes": [
            {"id": "start", "kind": "start"},
            {"id": "q1", "kind": "query_raw", "data": {"config": {}}},
            {"id": "end", "kind": "end"},
        ],
        "edges": [
            {"source": "start", "target": "q1"},
            {"source": "q1", "target": "end"},
        ],
    }
    with pytest.raises(CanvasCompileError, match="source_raw_db and"):
        compile_canvas_dag(canvas)


def test_query_raw_accepts_source_raw_db_and_table_key() -> None:
    canvas = {
        "nodes": [
            {"id": "start", "kind": "start"},
            {
                "id": "q1",
                "kind": "query_raw",
                "data": {"config": {"source_raw_db": "db1", "source_raw_table_key": "tbl1"}},
            },
            {"id": "end", "kind": "end"},
        ],
        "edges": [
            {"source": "start", "target": "q1"},
            {"source": "q1", "target": "end"},
        ],
    }
    compiled = compile_canvas_dag(canvas)
    assert any(t["id"] == "q1" for t in compiled["tasks"])


def test_validate_canvas_dag_collects_multiple_errors() -> None:
    canvas = {
        "nodes": [
            {"id": "start", "kind": "start"},
            {
                "id": "q1",
                "kind": "query_raw",
                "data": {"config": {}},
            },
            {
                "id": "t1",
                "kind": "transform",
                "data": {"config": {"handler_id": "trim_whitespace", "fields": [{"field_name": "name"}]}},
            },
            {"id": "end", "kind": "end"},
        ],
        "edges": [
            {"source": "start", "target": "q1"},
            {"source": "q1", "target": "t1"},
            {"source": "t1", "target": "end"},
        ],
    }
    errors = validate_canvas_dag(canvas)
    assert any("query_raw node 'q1'" in err for err in errors)
    assert any("transform node 't1': output_field is required" in err for err in errors)


def test_lookup_full_scan_rejected_on_non_query_node() -> None:
    canvas = {
        "nodes": [
            {"id": "start", "kind": "start"},
            {
                "id": "t1",
                "kind": "transform",
                "data": {
                    "config": {
                        "lookup_full_scan": True,
                        "handler_id": "trim_whitespace",
                        "trim_whitespace": {"mode": "ends_only"},
                        "output_field": "aliases",
                        "fields": [{"field_name": "name"}],
                    }
                },
            },
            {"id": "end", "kind": "end"},
        ],
        "edges": [
            {"source": "start", "target": "t1"},
            {"source": "t1", "target": "end"},
        ],
    }
    with pytest.raises(CanvasCompileError, match="lookup_full_scan is only supported on query nodes"):
        compile_canvas_dag(canvas)
