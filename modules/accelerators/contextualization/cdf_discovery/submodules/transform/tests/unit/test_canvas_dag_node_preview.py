from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag  # noqa: E402


def test_node_preview_not_compiled() -> None:
    compiled = compile_canvas_dag(
        {
            "nodes": [
                {
                    "id": "q1",
                    "kind": "query_view",
                    "data": {"config": {"view_external_id": "Asset"}},
                },
                {"id": "pv1", "kind": "node_preview", "data": {"config": {}}},
            ],
            "edges": [{"source": "q1", "target": "pv1"}],
        }
    )
    task_ids = {t["id"] for t in compiled["tasks"]}
    assert "pv1" not in task_ids
    assert "q1" in task_ids


def test_inline_preview_preserves_downstream_depends_on() -> None:
    compiled = compile_canvas_dag(
        {
            "nodes": [
                {
                    "id": "q1",
                    "kind": "query_view",
                    "data": {"config": {"view_external_id": "Asset"}},
                },
                {
                    "id": "t1",
                    "kind": "transform",
                    "data": {"config": {"enabled": False, "steps": []}},
                },
                {"id": "pv1", "kind": "node_preview", "data": {"config": {}}},
            ],
            "edges": [
                {"source": "q1", "target": "t1"},
                {"source": "t1", "target": "pv1"},
            ],
        }
    )
    transform = next(t for t in compiled["tasks"] if t["id"] == "t1")
    assert transform["depends_on"] == ["q1"]

    compiled_inline = compile_canvas_dag(
        {
            "nodes": [
                {
                    "id": "q1",
                    "kind": "query_view",
                    "data": {"config": {"view_external_id": "Asset"}},
                },
                {"id": "pv1", "kind": "node_preview", "data": {"config": {}}},
                {
                    "id": "t1",
                    "kind": "transform",
                    "data": {"config": {"enabled": False, "steps": []}},
                },
            ],
            "edges": [
                {"source": "q1", "target": "pv1"},
                {"source": "pv1", "target": "t1"},
            ],
        }
    )
    transform_inline = next(t for t in compiled_inline["tasks"] if t["id"] == "t1")
    assert transform_inline["depends_on"] == ["q1"]
