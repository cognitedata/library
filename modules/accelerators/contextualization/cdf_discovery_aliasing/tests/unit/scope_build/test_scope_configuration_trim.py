"""Tests for ``trim_scope_document_for_trigger_input`` (deploy-sized configuration)."""

from __future__ import annotations

import sys
from pathlib import Path

_PKG = Path(__file__).resolve().parents[3]
_SCRIPTS = _PKG / "scripts"
for _p in (_PKG, _SCRIPTS):
    s = str(_p)
    if s not in sys.path:
        sys.path.insert(0, s)

from scope_build.scope_configuration_trim import trim_scope_document_for_trigger_input


def test_trim_drops_compiled_workflow() -> None:
    doc = {
        "compiled_workflow": {"tasks": [{"id": "t1"}]},
        "canvas": {"nodes": [], "edges": []},
    }
    out = trim_scope_document_for_trigger_input(doc)
    assert "compiled_workflow" not in out


def test_trim_clears_discovery_scope_lists_when_keys_present() -> None:
    doc = {
        "canvas": {
            "nodes": [
                {
                    "id": "n1",
                    "kind": "query_view",
                    "data": {
                        "config": {
                            "description": "x",
                            "view_space": "s",
                            "view_external_id": "V",
                            "view_version": "v1",
                        }
                    },
                },
            ],
            "edges": [],
        },
        "view_queries": [{"legacy": True}],
        "transforms": [{"legacy": True}],
        "validations": [{"legacy": True}],
    }
    out = trim_scope_document_for_trigger_input(doc)
    assert out["view_queries"] == []
    assert out["transforms"] == []
    assert out["validations"] == []


def test_trim_clears_view_queries_when_canvas_empty() -> None:
    """Deploy trim clears discovery lists whenever the scope has a ``canvas`` block."""
    doc = {
        "canvas": {"nodes": [], "edges": []},
        "view_queries": [{"q": 0}, {"q": 1}],
    }
    out = trim_scope_document_for_trigger_input(doc)
    assert out["view_queries"] == []


def test_trim_prunes_source_views_from_associations() -> None:
    doc = {
        "associations": [
            {
                "kind": "source_view_to_extraction",
                "source_view_index": 0,
                "extraction_rule_name": "rA",
            },
            {
                "kind": "source_view_to_extraction",
                "source_view_index": 2,
                "extraction_rule_name": "rB",
            },
        ],
        "source_views": [{"i": 0}, {"i": 1}, {"i": 2}],
        "canvas": {"nodes": [], "edges": []},
    }
    out = trim_scope_document_for_trigger_input(doc)
    assert out["source_views"] == [{"i": 0}, {"i": 2}]
    assert out["associations"][0]["source_view_index"] == 0
    assert out["associations"][1]["source_view_index"] == 1


def test_trim_remaps_source_view_canvas_nodes_and_preserves_other_association_rows() -> None:
    doc = {
        "associations": [
            {
                "kind": "source_view_to_extraction",
                "source_view_index": 2,
                "extraction_rule_name": "x",
            },
            {"kind": "other_kind", "payload": True},
        ],
        "source_views": [{"a": 0}, {"a": 1}, {"a": 2}],
        "canvas": {
            "nodes": [
                {
                    "id": "sv2",
                    "kind": "source_view",
                    "data": {"ref": {"source_view_index": 2}},
                },
            ],
            "edges": [],
        },
    }
    out = trim_scope_document_for_trigger_input(doc)
    assert out["source_views"] == [{"a": 2}]
    assert out["canvas"]["nodes"][0]["data"]["ref"]["source_view_index"] == 0
    kinds = [x.get("kind") for x in out["associations"] if isinstance(x, dict)]
    assert "other_kind" in kinds


def test_trim_leaves_source_views_without_association_or_source_view_nodes() -> None:
    doc = {
        "source_views": [{"k": 0}, {"k": 1}],
        "canvas": {"nodes": [], "edges": []},
    }
    out = trim_scope_document_for_trigger_input(doc)
    assert out["source_views"] == [{"k": 0}, {"k": 1}]


def test_trim_source_views_passes_validate_workflow_associations() -> None:
    from functions.cdf_fn_common.workflow_associations import validate_workflow_associations

    doc = {
        "key_extraction": {
            "config": {
                "data": {
                    "extraction_rules": [
                        {"name": "rA"},
                        {"name": "rB"},
                    ],
                }
            }
        },
        "associations": [
            {
                "kind": "source_view_to_extraction",
                "source_view_index": 0,
                "extraction_rule_name": "rA",
            },
            {
                "kind": "source_view_to_extraction",
                "source_view_index": 2,
                "extraction_rule_name": "rB",
            },
        ],
        "source_views": [{"i": 0}, {"i": 1}, {"i": 2}],
        "canvas": {"nodes": [], "edges": []},
    }
    out = trim_scope_document_for_trigger_input(doc)
    assert validate_workflow_associations(out) == []


def test_trim_then_compile_discovery_only_canvas() -> None:
    from functions.cdf_fn_common.workflow_compile.canvas_dag import compiled_workflow_for_scope_document

    doc = {
        "compile_workflow_dag": "canvas",
        "view_queries": [{"a": 0}, {"a": 1}],
        "transforms": [{"t": 0}, {"t": 1}, {"t": 2}],
        "canvas": {
            "nodes": [
                {"id": "st", "kind": "start"},
                {
                    "id": "vq",
                    "kind": "query_view",
                    "data": {
                        "config": {
                            "description": "q",
                            "view_space": "cdf_cdm",
                            "view_external_id": "CogniteFile",
                            "view_version": "v1",
                        }
                    },
                },
                {
                    "id": "tr",
                    "kind": "transform",
                    "data": {"config": {"description": "stage"}},
                },
                {"id": "en", "kind": "end"},
            ],
            "edges": [
                {"source": "st", "target": "vq"},
                {"source": "vq", "target": "tr"},
                {"source": "tr", "target": "en"},
            ],
        },
    }
    out = trim_scope_document_for_trigger_input(doc)
    assert out["view_queries"] == []
    assert out["transforms"] == []
    cw = compiled_workflow_for_scope_document(out)
    tasks = cw.get("tasks") or []
    assert len(tasks) == 3
    by_fn = {t["function_external_id"]: t for t in tasks if isinstance(t, dict)}
    assert "fn_dm_view_query" in by_fn
    assert "fn_dm_transform" in by_fn
    assert "fn_dm_discovery_raw_cleanup" in by_fn
    assert by_fn["fn_dm_transform"]["depends_on"] == [by_fn["fn_dm_view_query"]["id"]]
    assert by_fn["fn_dm_discovery_raw_cleanup"]["depends_on"] == [by_fn["fn_dm_transform"]["id"]]
    assert by_fn["fn_dm_view_query"]["payload"]["config"]["view_external_id"] == "CogniteFile"
    assert by_fn["fn_dm_transform"]["payload"] == {"config": {"description": "stage"}}
