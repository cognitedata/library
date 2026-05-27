from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag  # noqa: E402
from cdf_fn_common.workflow_compile.codegen import build_workflow_version_document  # noqa: E402


def _minimal_canvas(*nodes: dict) -> dict:
    ids = [str(n["id"]) for n in nodes]
    edges = (
        [{"source": "start", "target": ids[0]}]
        + [{"source": ids[i], "target": ids[i + 1]} for i in range(len(ids) - 1)]
        + [{"source": ids[-1], "target": "end"}]
    )
    return {
        "nodes": [{"id": "start", "kind": "start"}, *nodes, {"id": "end", "kind": "end"}],
        "edges": edges,
    }


def test_function_ref_compiles_function_external_id() -> None:
    canvas = _minimal_canvas(
        {
            "id": "fn1",
            "kind": "function_ref",
            "data": {"config": {"function_external_id": "fn_custom"}},
        }
    )
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "fn1")
    assert task["task_type"] == "function"
    assert task["executable_kind"] == "function_ref"
    assert task["function_external_id"] == "fn_custom"
    wv = build_workflow_version_document(
        workflow_external_id="wf_test",
        version="1",
        compiled_workflow=compiled,
    )
    wf_task = next(
        t
        for t in wv["workflowDefinition"]["tasks"]
        if t["externalId"] == "fn1"
    )
    assert wf_task["type"] == "function"
    assert wf_task["parameters"]["function"]["externalId"] == "fn_custom"


def test_transformation_ref_emits_native_transformation_task() -> None:
    canvas = _minimal_canvas(
        {
            "id": "tr1",
            "kind": "transformation_ref",
            "data": {"config": {"transformation_external_id": "tr_load_assets"}},
        }
    )
    compiled = compile_canvas_dag(canvas)
    wv = build_workflow_version_document(
        workflow_external_id="wf_test",
        version="1",
        compiled_workflow=compiled,
    )
    task = next(
        t
        for t in wv["workflowDefinition"]["tasks"]
        if t["externalId"] == "tr1"
    )
    assert task["type"] == "transformation"
    assert task["parameters"]["transformation"]["externalId"] == "tr_load_assets"


def test_subworkflow_emits_native_subworkflow_task() -> None:
    canvas = _minimal_canvas(
        {
            "id": "sw1",
            "kind": "subworkflow",
            "data": {
                "config": {
                    "workflow_external_id": "wf_child",
                    "workflow_version": "2",
                }
            },
        }
    )
    compiled = compile_canvas_dag(canvas)
    wv = build_workflow_version_document(
        workflow_external_id="wf_test",
        version="1",
        compiled_workflow=compiled,
    )
    task = next(
        t
        for t in wv["workflowDefinition"]["tasks"]
        if t["externalId"] == "sw1"
    )
    assert task["type"] == "subworkflow"
    assert task["parameters"]["subworkflow"]["externalId"] == "wf_child"
    assert task["parameters"]["subworkflow"]["version"] == "2"
