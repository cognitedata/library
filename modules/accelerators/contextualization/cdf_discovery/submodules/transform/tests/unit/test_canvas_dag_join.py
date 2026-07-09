"""Tests for join wiring in canvas compile."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
if str(FUNCS) not in sys.path:
    sys.path.insert(0, str(FUNCS))

from cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: E402
    CanvasCompileError,
    compile_canvas_dag,
)


def test_compile_join_sets_left_and_right_task_ids() -> None:
    canvas = {
        "nodes": [
            {"id": "left_q", "kind": "query_view", "data": {"config": {"view_external_id": "Asset"}}},
            {"id": "right_q", "kind": "query_raw", "data": {"config": {"source_raw_db": "db", "source_raw_table_key": "t"}}},
            {
                "id": "jn",
                "kind": "join",
                "data": {
                    "config": {
                        "join_on": {"and": [{"operator": "EQUALS", "left_property": "name", "right_property": "raw_columns.name"}]},
                    }
                },
            },
        ],
        "edges": [
            {"source": "left_q", "target": "jn", "target_handle": "in__left"},
            {"source": "right_q", "target": "jn", "target_handle": "in__right"},
        ],
    }
    compiled = compile_canvas_dag(canvas)
    join_task = next(t for t in compiled["tasks"] if t["id"] == "jn")
    assert join_task["payload"]["join_left_task_id"] == "left_q"
    assert join_task["payload"]["join_right_task_id"] == "right_q"


def test_compile_join_requires_both_handles() -> None:
    canvas = {
        "nodes": [
            {"id": "left_q", "kind": "query_view", "data": {"config": {}}},
            {"id": "jn", "kind": "join", "data": {"config": {"join_on": {"and": []}}}},
        ],
        "edges": [{"source": "left_q", "target": "jn", "target_handle": "in__left"}],
    }
    try:
        compile_canvas_dag(canvas)
        raise AssertionError("expected CanvasCompileError")
    except CanvasCompileError as exc:
        assert "in__right" in str(exc)
