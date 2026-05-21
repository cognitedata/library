"""Tests for canvas cascade disable/enable (orphaned downstream nodes)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_MODULE_ROOT = Path(__file__).resolve().parents[3]
_FUNCS = _MODULE_ROOT / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.workflow_compile.canvas_enablement import (  # noqa: E402
    apply_canvas_node_enablement_patch,
    cascade_disable_ids,
    cascade_enable_ids,
    is_canvas_node_cascade_disabled,
    is_canvas_node_enabled,
)

_VQ = {
    "id": "vq",
    "kind": "query_view",
    "data": {
        "config": {
            "description": "q0",
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteFile",
            "view_version": "v1",
        }
    },
}
_VQ_B = {
    "id": "vqB",
    "kind": "query_view",
    "data": {
        "config": {
            "description": "q1",
            "view_space": "cdf_cdm",
            "view_external_id": "CogniteFile",
            "view_version": "v1",
        }
    },
}
_TR = {"id": "tr", "kind": "transform", "data": {"config": {"description": "t0"}}}
_VA = {"id": "va", "kind": "validation", "data": {"config": {"description": "v0"}}}


def _canvas(nodes, edges):
    return {"schemaVersion": 1, "nodes": nodes, "edges": edges}


def test_cascade_disable_linear_chain():
    canvas = _canvas(
        [_VQ, _TR, _VA],
        [
            {"source": "vq", "target": "tr"},
            {"source": "tr", "target": "va"},
        ],
    )
    disabled = cascade_disable_ids(canvas, {"vq"})
    assert disabled == {"vq", "tr", "va"}


def test_cascade_disable_parallel_join_one_side():
    join = {
        "id": "jn",
        "kind": "join",
        "data": {
            "config": {
                "description": "j",
                "join_on": {"operator": "EQUALS", "left_property": "a", "right_property": "b"},
            }
        },
    }
    canvas = _canvas(
        [_VQ, _VQ_B, join],
        [
            {"source": "vq", "target": "jn", "target_handle": "in__left"},
            {"source": "vqB", "target": "jn", "target_handle": "in__right"},
        ],
    )
    disabled = cascade_disable_ids(canvas, {"vq"})
    assert "jn" not in disabled
    assert "vqB" not in disabled
    disabled_both = cascade_disable_ids(canvas, {"vq", "vqB"})
    assert disabled_both == {"vq", "vqB", "jn"}


def test_cascade_enable_restores_only_cascade_marked():
    canvas = _canvas(
        [_VQ, _TR, _VA],
        [
            {"source": "vq", "target": "tr"},
            {"source": "tr", "target": "va"},
        ],
    )
    canvas2, _ = apply_canvas_node_enablement_patch(canvas, root_id="vq", enabled=False)
    by_id = {n["id"]: n for n in canvas2["nodes"]}
    assert not is_canvas_node_enabled(by_id["tr"])
    assert is_canvas_node_cascade_disabled(by_id["tr"])

    canvas3, _ = apply_canvas_node_enablement_patch(canvas2, root_id="vq", enabled=True)
    by_id3 = {n["id"]: n for n in canvas3["nodes"]}
    assert is_canvas_node_enabled(by_id3["vq"])
    assert is_canvas_node_enabled(by_id3["tr"])
    assert is_canvas_node_enabled(by_id3["va"])


def test_manual_disable_not_cascade_enabled():
    canvas = _canvas(
        [_VQ, _TR],
        [{"source": "vq", "target": "tr"}],
    )
    canvas2, _ = apply_canvas_node_enablement_patch(canvas, root_id="vq", enabled=False)
    canvas3, _ = apply_canvas_node_enablement_patch(
        dict(canvas2),
        root_id="tr",
        enabled=False,
    )
    by_id = {n["id"]: n for n in canvas3["nodes"]}
    assert not is_canvas_node_cascade_disabled(by_id["tr"])

    canvas4, _ = apply_canvas_node_enablement_patch(canvas3, root_id="vq", enabled=True)
    by_id4 = {n["id"]: n for n in canvas4["nodes"]}
    assert is_canvas_node_enabled(by_id4["vq"])
    assert not is_canvas_node_enabled(by_id4["tr"])


def test_entry_query_downstream_disabled_only():
    canvas = _canvas(
        [
            {"id": "st", "kind": "start"},
            _VQ,
            _TR,
        ],
        [
            {"source": "st", "target": "vq"},
            {"source": "vq", "target": "tr"},
        ],
    )
    disabled = cascade_disable_ids(canvas, {"vq"})
    assert disabled == {"vq", "tr"}
    assert "st" not in disabled


def test_cascade_enable_ids_requires_valid_upstream():
    canvas = _canvas([_VQ, _TR], [{"source": "vq", "target": "tr"}])
    canvas2, _ = apply_canvas_node_enablement_patch(canvas, root_id="vq", enabled=False)
    assert cascade_enable_ids(canvas2, {"vq"}) == set()
    canvas3, affected = apply_canvas_node_enablement_patch(canvas2, root_id="vq", enabled=True)
    assert affected == {"tr"}
    assert is_canvas_node_enabled({n["id"]: n for n in canvas3["nodes"]}["tr"])


def test_cascade_disable_propagates_through_subgraph_inner_canvas():
    sg = {
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
    }
    tr2 = {"id": "tr2", "kind": "transform", "data": {"config": {"description": "t1"}}}
    canvas = _canvas(
        [_VQ, sg, tr2],
        [
            {"source": "vq", "target": "sg", "target_handle": "in__in"},
            {"source": "sg", "target": "tr2", "source_handle": "out__out"},
        ],
    )
    canvas2, _ = apply_canvas_node_enablement_patch(canvas, root_id="vq", enabled=False)
    by_id = {n["id"]: n for n in canvas2["nodes"]}
    assert not is_canvas_node_enabled(by_id["sg"])
    assert is_canvas_node_cascade_disabled(by_id["sg"])
    assert not is_canvas_node_enabled(by_id["tr2"])
    inner = by_id["sg"]["data"]["inner_canvas"]
    rq = next(n for n in inner["nodes"] if n["id"] == "rq")
    assert not is_canvas_node_enabled(rq)
    assert is_canvas_node_cascade_disabled(rq)
