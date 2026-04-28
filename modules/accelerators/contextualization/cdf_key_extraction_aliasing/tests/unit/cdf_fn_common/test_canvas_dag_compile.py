"""Tests for canvas → ``compiled_workflow`` DAG compilation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: E402
    CanvasCompileError,
    compile_canvas_dag,
    compile_workflow_from_document,
    should_use_canvas_dag,
)
from cdf_fn_common.workflow_compile.legacy_ir import TASK_INCREMENTAL  # noqa: E402


def _minimal_scope_doc() -> dict:
    return {
        "schemaVersion": 1,
        "source_views": [
            {
                "view_space": "cdf_cdm",
                "view_external_id": "CogniteFile",
                "view_version": "v1",
            }
        ],
        "key_extraction": {"config": {"parameters": {"raw_db": "db_ke", "raw_table_key": "tkey"}}},
        "aliasing": {"config": {"parameters": {"raw_db": "db_al", "raw_table_aliases": "tal"}}},
    }


def test_should_use_canvas_dag_auto() -> None:
    doc = _minimal_scope_doc()
    assert should_use_canvas_dag(doc) is False
    doc["canvas"] = {"nodes": [{"id": "x", "kind": "start"}], "edges": []}
    assert should_use_canvas_dag(doc) is False
    doc["canvas"]["nodes"].append({"id": "e1", "kind": "extraction", "data": {}})
    assert should_use_canvas_dag(doc) is True


def test_should_use_canvas_dag_true_when_executable_only_inside_subgraph() -> None:
    doc = _minimal_scope_doc()
    doc["canvas"] = {
        "nodes": [
            {
                "id": "sg",
                "kind": "subgraph",
                "data": {
                    "inner_canvas": {
                        "nodes": [{"id": "ex1", "kind": "extraction", "data": {}}],
                        "edges": [],
                    }
                },
            }
        ],
        "edges": [],
    }
    assert should_use_canvas_dag(doc) is True


def test_compile_canvas_linear_chain() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {
        "nodes": [
            {"id": "st", "kind": "start"},
            {"id": "ex", "kind": "extraction", "data": {"ref": {"extraction_rule_name": "r1"}}},
            {"id": "al", "kind": "aliasing", "data": {"ref": {"aliasing_rule_name": "a1"}}},
        ],
        "edges": [
            {"source": "st", "target": "ex"},
            {"source": "ex", "target": "al"},
        ],
    }
    cw = compile_canvas_dag(doc)
    assert cw.get("dag_source") == "canvas"
    ids = [t["id"] for t in cw["tasks"]]
    assert TASK_INCREMENTAL in ids
    ex_tid = next(t["id"] for t in cw["tasks"] if t.get("canvas_node_id") == "ex")
    al_tid = next(t["id"] for t in cw["tasks"] if t.get("canvas_node_id") == "al")
    ex_task = next(t for t in cw["tasks"] if t["id"] == ex_tid)
    al_task = next(t for t in cw["tasks"] if t["id"] == al_tid)
    assert TASK_INCREMENTAL in ex_task["depends_on"]
    assert ex_tid in al_task["depends_on"]


def test_compile_canvas_cycle_errors() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {
        "nodes": [
            {"id": "a", "kind": "extraction", "data": {}},
            {"id": "b", "kind": "extraction", "data": {}},
        ],
        "edges": [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "a"},
        ],
    }
    with pytest.raises(CanvasCompileError, match="cycle"):
        compile_canvas_dag(doc)


def test_compile_workflow_from_document_auto_requires_executable_canvas() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "auto"
    doc["canvas"] = {"nodes": [], "edges": []}
    with pytest.raises(CanvasCompileError, match="no executable nodes"):
        compile_workflow_from_document(doc)


def test_compile_workflow_from_document_auto_no_canvas_errors() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "auto"
    with pytest.raises(CanvasCompileError, match="canvas is missing"):
        compile_workflow_from_document(doc)


def test_compile_workflow_from_document_flattens_subgraph_boundary_edges() -> None:
    """Parent edges to/from the subgraph frame attach to inner graph in / out hubs."""
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "auto"
    doc["canvas"] = {
        "nodes": [
            {"id": "ex", "kind": "extraction", "data": {"ref": {"extraction_rule_name": "r1"}}},
            {
                "id": "sg",
                "kind": "subgraph",
                "data": {
                    "subflow_hub_input_id": "sg_in",
                    "subflow_hub_output_id": "sg_out",
                    "inner_canvas": {
                        "nodes": [
                            {"id": "sg_in", "kind": "subflow_graph_in", "data": {}},
                            {
                                "id": "ex2",
                                "kind": "extraction",
                                "data": {"ref": {"extraction_rule_name": "r2"}},
                            },
                            {"id": "sg_out", "kind": "subflow_graph_out", "data": {}},
                        ],
                        "edges": [
                            {
                                "source": "sg_in",
                                "target": "ex2",
                                "source_handle": "out__in",
                                "target_handle": "in",
                            },
                            {
                                "source": "ex2",
                                "target": "sg_out",
                                "source_handle": "out",
                                "target_handle": "in__out",
                            },
                        ],
                    },
                },
            },
            {"id": "al", "kind": "aliasing", "data": {"ref": {"aliasing_rule_name": "a1"}}},
        ],
        "edges": [
            {"source": "ex", "target": "sg", "target_handle": "in__in"},
            {"source": "sg", "target": "al", "source_handle": "out__out"},
        ],
    }
    cw = compile_workflow_from_document(doc)
    by_cn = {t["canvas_node_id"]: t for t in cw["tasks"] if t.get("canvas_node_id")}
    assert set(by_cn) == {"ex", "ex2", "al"}
    assert TASK_INCREMENTAL in by_cn["ex"]["depends_on"]
    assert TASK_INCREMENTAL in by_cn["ex2"]["depends_on"]
    assert by_cn["ex"]["id"] in by_cn["ex2"]["depends_on"]
    assert by_cn["ex2"]["id"] in by_cn["al"]["depends_on"]


def test_compile_subgraph_missing_inner_raises() -> None:
    doc = _minimal_scope_doc()
    doc["canvas"] = {
        "nodes": [
            {"id": "sg", "kind": "subgraph", "data": {"subflow_hub_input_id": "a", "subflow_hub_output_id": "b"}},
            {"id": "ex", "kind": "extraction", "data": {"ref": {"extraction_rule_name": "r1"}}},
        ],
        "edges": [],
    }
    with pytest.raises(CanvasCompileError, match="inner_canvas"):
        compile_canvas_dag(doc)


def test_compile_workflow_from_document_legacy_rejected() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "legacy"
    with pytest.raises(ValueError, match="legacy"):
        compile_workflow_from_document(doc)


def test_should_use_canvas_dag_false_when_legacy_requested() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "legacy"
    assert should_use_canvas_dag(doc) is False


def test_compile_workflow_from_document_canvas_mode_errors() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {"nodes": [], "edges": []}
    with pytest.raises(CanvasCompileError):
        compile_workflow_from_document(doc)
