"""Tests for recursive source view filter building (AND/OR/NOT, PREFIX, RANGE)."""

from __future__ import annotations

import sys
from pathlib import Path

from cognite.client.data_classes.data_modeling.ids import ViewId

_FUNCS = Path(__file__).resolve().parents[3] / "functions"
if str(_FUNCS) not in sys.path:
    sys.path.insert(0, str(_FUNCS))

from cdf_fn_common.source_view_filter_build import (
    build_source_view_query_filter,
    filter_dict_to_dm,
)


def test_filter_dict_nested_and_or_dump() -> None:
    vid = ViewId(space="vs", external_id="V", version="v1")
    node = {
        "and": [
            {"operator": "EQUALS", "target_property": "equipmentType", "values": ["pump"]},
            {
                "or": [
                    {"operator": "EQUALS", "target_property": "zone", "values": ["A"]},
                    {"operator": "EQUALS", "target_property": "zone", "values": ["B"]},
                ]
            },
        ],
    }
    f = filter_dict_to_dm(vid, node)
    dumped = f.dump()
    assert "and" in dumped


def test_filter_dict_not_leaf() -> None:
    vid = ViewId(space="vs", external_id="V", version="v1")
    node = {
        "not": {"operator": "EQUALS", "target_property": "deprecated", "values": [True]}
    }
    f = filter_dict_to_dm(vid, node)
    dumped = f.dump()
    assert "not" in dumped


def test_build_query_includes_has_data() -> None:
    vid = ViewId(space="vs", external_id="V", version="v1")
    f = build_source_view_query_filter(vid, [])
    dumped = f.dump()
    assert "hasData" in dumped
