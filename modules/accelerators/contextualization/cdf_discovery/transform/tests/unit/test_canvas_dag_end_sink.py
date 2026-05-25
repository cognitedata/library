from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag  # noqa: E402


def _canvas_with_end() -> dict:
    return {
        "nodes": [
            {"id": "start", "kind": "start"},
            {
                "id": "q1",
                "kind": "query_view",
                "data": {"config": {"view_external_id": "Asset"}},
            },
            {"id": "end", "kind": "end", "data": {"config": {"description": "cleanup"}}},
        ],
        "edges": [
            {"source": "start", "target": "q1"},
            {"source": "q1", "target": "end"},
        ],
    }


def test_end_node_compiles_as_raw_cleanup_sink() -> None:
    compiled = compile_canvas_dag(_canvas_with_end())
    tasks = compiled["tasks"]
    cleanup = [t for t in tasks if t.get("executable_kind") == "raw_cleanup"]
    assert len(cleanup) == 1
    assert cleanup[0]["id"] == "end"
    assert cleanup[0]["canvas_node_id"] == "end"
    assert cleanup[0]["depends_on"] == ["q1"]
    assert cleanup[0]["function_external_id"] == "fn_etl_raw_cleanup"
    assert "etl__raw_cleanup__sink" not in {t["id"] for t in tasks}


def test_no_end_node_auto_injects_sink() -> None:
    compiled = compile_canvas_dag(
        {
            "nodes": [
                {
                    "id": "q1",
                    "kind": "query_view",
                    "data": {"config": {"view_external_id": "Asset"}},
                }
            ],
            "edges": [],
        }
    )
    cleanup = [t for t in compiled["tasks"] if t.get("executable_kind") == "raw_cleanup"]
    assert len(cleanup) == 1
    assert cleanup[0]["id"] == "etl__raw_cleanup__sink"
