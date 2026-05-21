"""Unit tests for discovery row filter stage."""

from __future__ import annotations

import pytest

from cdf_fn_common.discovery_row_filter import row_passes_filter, validate_filter_config


def test_row_passes_filter_any_one_property() -> None:
    filters = [
        {
            "or": [
                {"operator": "EXISTS", "target_property": "aliases", "property_scope": "view"},
                {"operator": "EXISTS", "target_property": "indexKey", "property_scope": "view"},
            ]
        }
    ]
    props = {}
    assert row_passes_filter(props, filters) is False
    props2 = {"indexKey": [{"value": "k", "confidence": 0.5}]}
    assert row_passes_filter(props2, filters) is True


def test_row_passes_filter_all_requires_every_property() -> None:
    filters = [
        {
            "and": [
                {"operator": "EXISTS", "target_property": "aliases"},
                {"operator": "EXISTS", "target_property": "indexKey"},
            ]
        }
    ]
    props = {
        "aliases": [{"value": "a", "confidence": 0.9}],
    }
    assert row_passes_filter(props, filters) is False
    props_ok = {
        "aliases": [{"value": "a", "confidence": 0.9}],
        "indexKey": [{"value": "k", "confidence": 0.85}],
    }
    assert row_passes_filter(props_ok, filters) is True


def test_validate_filter_config_requires_filters() -> None:
    with pytest.raises(ValueError, match="filters"):
        validate_filter_config({"description": "x"})
    validate_filter_config(
        {
            "description": "gate",
            "filters": [{"operator": "EXISTS", "target_property": "aliases"}],
        }
    )


def test_compile_canvas_instance_filter_kind() -> None:
    from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag

    doc = {
        "compile_workflow_dag": "canvas",
        "canvas": {
            "nodes": [
                {"id": "tr", "kind": "transform", "data": {"config": {"description": "t0"}}},
                {
                    "id": "fl",
                    "kind": "instance_filter",
                    "data": {
                        "config": {
                            "description": "gate",
                            "filters": [
                                {"operator": "EXISTS", "target_property": "aliases"},
                            ],
                        }
                    },
                },
            ],
            "edges": [{"source": "tr", "target": "fl"}],
        },
    }
    cw = compile_canvas_dag(doc)
    by_cn = {t["canvas_node_id"]: t for t in cw["tasks"] if t.get("canvas_node_id")}
    assert by_cn["fl"]["function_external_id"] == "fn_dm_filter"
    assert by_cn["fl"]["depends_on"] == [by_cn["tr"]["id"]]
