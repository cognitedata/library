"""Tests for canvas → ``compiled_workflow`` DAG compilation (discovery pipeline)."""

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
    discovery_local_pipeline_specs,
)

_VQ_DATA = {
    "config": {
        "description": "q0",
        "view_space": "cdf_cdm",
        "view_external_id": "CogniteFile",
        "view_version": "v1",
    }
}


def test_discovery_local_pipeline_specs_matches_canvas_functions() -> None:
    from cdf_fn_common.workflow_compile.canvas_dag import _KIND_FN

    specs = discovery_local_pipeline_specs()
    assert set(specs.keys()) == {fn for fn, _ in _KIND_FN.values()}
    for _kind, (fn_ext, exec_kind) in _KIND_FN.items():
        assert specs[fn_ext] == (f"{fn_ext}.pipeline", exec_kind), _kind


_TR_DATA = {"config": {"description": "t0"}}


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
    }


def test_compile_canvas_linear_discovery_chain() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {
        "nodes": [
            {"id": "st", "kind": "start"},
            {"id": "vq", "kind": "query_view", "data": dict(_VQ_DATA)},
            {"id": "tr", "kind": "transform", "data": dict(_TR_DATA)},
        ],
        "edges": [
            {"source": "st", "target": "vq"},
            {"source": "vq", "target": "tr"},
        ],
    }
    cw = compile_canvas_dag(doc)
    assert cw.get("dag_source") == "canvas"
    by_cn = {t["canvas_node_id"]: t for t in cw["tasks"] if t.get("canvas_node_id")}
    tid_vq = by_cn["vq"]["id"]
    tid_tr = by_cn["tr"]["id"]
    assert tid_vq == "kea__query_view__q0"
    assert tid_tr == "kea__transform__t0"
    ids = [t["id"] for t in cw["tasks"]]
    assert ids == [tid_vq, tid_tr, "kea__discovery_raw_cleanup"]
    vq = by_cn["vq"]
    tr = by_cn["tr"]
    cl = next(t for t in cw["tasks"] if t.get("id") == "kea__discovery_raw_cleanup")
    assert vq["function_external_id"] == "fn_dm_view_query"
    assert vq["depends_on"] == []
    assert vq["payload"] == {"config": dict(_VQ_DATA["config"])}
    assert tr["function_external_id"] == "fn_dm_transform"
    assert tr["depends_on"] == [tid_vq]
    assert tr["payload"] == {"config": {"description": "t0"}}
    assert cl["function_external_id"] == "fn_dm_discovery_raw_cleanup"
    assert cl["depends_on"] == [tid_tr]


def test_compile_canvas_inverted_index_uses_discovery_predecessor_payload() -> None:
    """Inverted-index tasks carry IR payload pointing at compiled predecessor ids (local runner merges snapshots)."""
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {
        "nodes": [
            {"id": "vq", "kind": "query_view", "data": dict(_VQ_DATA)},
            {"id": "va", "kind": "validation", "data": {"config": {"description": "v1"}}},
            {
                "id": "ii",
                "kind": "inverted_index",
                "data": {
                    "config": {
                        "description": "Inverted index",
                        "index_kinds": {"metadata": ["discoveredKey"]},
                    }
                },
            },
        ],
        "edges": [
            {"source": "vq", "target": "va"},
            {"source": "va", "target": "ii"},
        ],
    }
    cw = compile_canvas_dag(doc)
    by_cn = {t["canvas_node_id"]: t for t in cw["tasks"] if isinstance(t, dict)}
    tid_va = by_cn["va"]["id"]
    ii = by_cn["ii"]
    assert ii["function_external_id"] == "fn_dm_inverted_index"
    assert ii["depends_on"] == [tid_va]
    pl = ii["payload"]
    assert pl["inverted_index_input_source"] == "discovery_predecessor_payloads"
    assert pl["upstream_compiled_task_ids"] == [tid_va]
    cleanup = next(t for t in cw["tasks"] if t.get("id") == "kea__discovery_raw_cleanup")
    assert cleanup["depends_on"] == [ii["id"]]


def test_compile_canvas_join_payload_left_right_task_ids() -> None:
    """Join tasks carry explicit predecessor compiled task ids from ``in__left`` / ``in__right`` edges."""
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {
        "nodes": [
            {"id": "st", "kind": "start"},
            {"id": "qL", "kind": "query_raw", "data": {"config": {"description": "left q"}}},
            {"id": "qR", "kind": "query_raw", "data": {"config": {"description": "right q"}}},
            {
                "id": "jn",
                "kind": "join",
                "data": {
                    "config": {
                        "description": "merge",
                        "join_type": "inner",
                        "join_on": {"operator": "EQUALS", "left_property": "a", "right_property": "b"},
                    }
                },
            },
        ],
        "edges": [
            {"source": "st", "target": "qL"},
            {"source": "st", "target": "qR"},
            {"source": "qL", "target": "jn", "target_handle": "in__left"},
            {"source": "qR", "target": "jn", "target_handle": "in__right"},
        ],
    }
    cw = compile_canvas_dag(doc)
    by_cn = {t["canvas_node_id"]: t for t in cw["tasks"] if isinstance(t, dict)}
    tid_qL = by_cn["qL"]["id"]
    tid_qR = by_cn["qR"]["id"]
    tid_jn = by_cn["jn"]["id"]
    jn = by_cn["jn"]
    assert jn["function_external_id"] == "fn_dm_join"
    assert set(jn["depends_on"]) == {tid_qL, tid_qR}
    pl = jn["payload"]
    assert pl["join_left_task_id"] == tid_qL
    assert pl["join_right_task_id"] == tid_qR
    assert pl["config"]["join_type"] == "inner"
    cleanup = next(t for t in cw["tasks"] if t.get("id") == "kea__discovery_raw_cleanup")
    assert set(cleanup["depends_on"]) == {tid_jn}


def test_compile_canvas_join_rejects_wrong_target_handle() -> None:
    doc = _minimal_scope_doc()
    doc["canvas"] = {
        "nodes": [
            {"id": "st", "kind": "start"},
            {"id": "qL", "kind": "query_raw", "data": {"config": {"description": "L"}}},
            {"id": "qR", "kind": "query_raw", "data": {"config": {"description": "R"}}},
            {
                "id": "jn",
                "kind": "join",
                "data": {
                    "config": {
                        "description": "j",
                        "join_on": {"operator": "EQUALS", "left_property": "a", "right_property": "b"},
                    }
                },
            },
        ],
        "edges": [
            {"source": "st", "target": "qL"},
            {"source": "st", "target": "qR"},
            {"source": "qL", "target": "jn", "target_handle": "in"},
            {"source": "qR", "target": "jn", "target_handle": "in__right"},
        ],
    }
    with pytest.raises(CanvasCompileError, match="in__left"):
        compile_canvas_dag(doc)


def test_compile_canvas_cycle_errors() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {
        "nodes": [
            {"id": "a", "kind": "query_view", "data": dict(_VQ_DATA)},
            {"id": "b", "kind": "query_view", "data": dict(_VQ_DATA)},
        ],
        "edges": [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "a"},
        ],
    }
    with pytest.raises(CanvasCompileError, match="cycle"):
        compile_canvas_dag(doc)


def test_human_readable_task_ids_disambiguate_duplicate_descriptions() -> None:
    """Two stages with the same description get distinct compiled task ids (hash suffix)."""
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {
        "nodes": [
            {"id": "st", "kind": "start"},
            {
                "id": "a",
                "kind": "query_view",
                "data": {
                    "config": {
                        "description": "same",
                        "view_space": "cdf_cdm",
                        "view_external_id": "CogniteFile",
                        "view_version": "v1",
                    }
                },
            },
            {
                "id": "b",
                "kind": "query_view",
                "data": {
                    "config": {
                        "description": "same",
                        "view_space": "cdf_cdm",
                        "view_external_id": "CogniteTimeSeries",
                        "view_version": "v1",
                    }
                },
            },
        ],
        "edges": [
            {"source": "st", "target": "a"},
            {"source": "st", "target": "b"},
        ],
    }
    cw = compile_canvas_dag(doc)
    ids = {t["id"] for t in cw["tasks"] if t.get("function_external_id") == "fn_dm_view_query"}
    assert len(ids) == 2
    assert all(s.startswith("kea__query_view__same") for s in ids)
    assert "kea__query_view__same" in ids
    assert any(s.startswith("kea__query_view__same__") and len(s) > len("kea__query_view__same") for s in ids)


def test_compile_workflow_from_document_auto_rejected() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "auto"
    with pytest.raises(ValueError, match="auto"):
        compile_workflow_from_document(doc)


def test_compile_workflow_from_document_requires_executable_canvas_when_canvas_set() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {"nodes": [], "edges": []}
    with pytest.raises(CanvasCompileError, match="no executable nodes"):
        compile_workflow_from_document(doc)


def test_compile_workflow_from_document_no_canvas_errors() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    with pytest.raises(CanvasCompileError, match="canvas is missing"):
        compile_workflow_from_document(doc)


def test_compile_workflow_from_document_flattens_subgraph_boundary_edges() -> None:
    """Parent edges to/from the subgraph frame attach to inner graph in / out hubs."""
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {
        "nodes": [
            {"id": "vq", "kind": "query_view", "data": dict(_VQ_DATA)},
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
                                "id": "rq",
                                "kind": "query_raw",
                                "data": {"config": {"description": "r0", "raw_db": "db"}},
                            },
                            {"id": "sg_out", "kind": "subflow_graph_out", "data": {}},
                        ],
                        "edges": [
                            {
                                "source": "sg_in",
                                "target": "rq",
                                "source_handle": "out__in",
                                "target_handle": "in",
                            },
                            {
                                "source": "rq",
                                "target": "sg_out",
                                "source_handle": "out",
                                "target_handle": "in__out",
                            },
                        ],
                    },
                },
            },
            {"id": "tr", "kind": "transform", "data": dict(_TR_DATA)},
        ],
        "edges": [
            {"source": "vq", "target": "sg", "target_handle": "in__in"},
            {"source": "sg", "target": "tr", "source_handle": "out__out"},
        ],
    }
    cw = compile_workflow_from_document(doc)
    by_cn = {
        t["canvas_node_id"]: t
        for t in cw["tasks"]
        if t.get("canvas_node_id")
        and t.get("function_external_id") != "fn_dm_discovery_raw_cleanup"
    }
    assert set(by_cn) == {"vq", "rq", "tr"}
    assert by_cn["vq"]["depends_on"] == []
    assert by_cn["rq"]["depends_on"] == [by_cn["vq"]["id"]]
    assert by_cn["tr"]["depends_on"] == [by_cn["rq"]["id"]]
    cleanup = next(t for t in cw["tasks"] if t.get("id") == "kea__discovery_raw_cleanup")
    assert cleanup["depends_on"] == [by_cn["tr"]["id"]]


def test_compile_subgraph_missing_inner_raises() -> None:
    doc = _minimal_scope_doc()
    doc["canvas"] = {
        "nodes": [
            {"id": "sg", "kind": "subgraph", "data": {"subflow_hub_input_id": "a", "subflow_hub_output_id": "b"}},
            {"id": "vq", "kind": "query_view", "data": dict(_VQ_DATA)},
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


def test_compile_workflow_from_document_canvas_mode_errors() -> None:
    doc = _minimal_scope_doc()
    doc["compile_workflow_dag"] = "canvas"
    doc["canvas"] = {"nodes": [], "edges": []}
    with pytest.raises(CanvasCompileError):
        compile_workflow_from_document(doc)
