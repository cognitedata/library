"""Unit tests for index_field_config DM query filter compilation."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm

from inverted_index.config import INDEX_FIELD_CONFIG
from inverted_index.dm_query import collect_view_property_paths
from inverted_index.view_query_filters import (
    compile_view_config_filters,
    filter_dict_to_dm,
    filter_target_property_paths,
    normalize_filter_operator,
)


def _view_id() -> SimpleNamespace:
    vid = SimpleNamespace(space="cdf_cdm", external_id="CogniteFile", version="v1")
    vid.as_property_ref = MagicMock(side_effect=lambda prop: ("view", prop))
    return vid


def test_normalize_filter_operator_aliases() -> None:
    assert normalize_filter_operator(">=") == "GTE"
    assert normalize_filter_operator("contains_any") == "CONTAINSANY"


def test_filter_target_property_paths_skips_node_scope() -> None:
    paths = filter_target_property_paths(
        [
            {
                "operator": "EQUALS",
                "target_property": "mimeType",
                "property_scope": "view",
                "values": ["application/pdf"],
            },
            {
                "operator": "EQUALS",
                "target_property": "space",
                "property_scope": "node",
                "values": ["springfield_instances"],
            },
        ]
    )
    assert paths == ["mimeType"]


def test_filter_target_property_paths_nested_and() -> None:
    paths = filter_target_property_paths(
        [
            {
                "and": [
                    {
                        "operator": "IN",
                        "target_property": "tags",
                        "values": ["ToAnnotate"],
                    },
                    {
                        "or": [
                            {
                                "operator": "EQUALS",
                                "target_property": "zone",
                                "values": ["A"],
                            },
                        ]
                    },
                ]
            }
        ]
    )
    assert paths == ["tags", "zone"]


def test_compile_view_config_filters_equals() -> None:
    view_id = _view_id()
    compiled = compile_view_config_filters(
        view_id,
        [
            {
                "operator": "EQUALS",
                "target_property": "mimeType",
                "values": ["application/pdf"],
            }
        ],
    )
    assert len(compiled) == 1
    view_id.as_property_ref.assert_called_with("mimeType")


def test_filter_dict_to_dm_unsupported_operator() -> None:
    with pytest.raises(ValueError, match="Unsupported filter operator"):
        filter_dict_to_dm(
            _view_id(),
            {"operator": "NOT_A_REAL_OP", "target_property": "name", "values": ["x"]},
        )


def test_collect_view_property_paths_includes_filter_properties() -> None:
    paths = collect_view_property_paths(
        view_external_id="CogniteFile",
        index_field_config=[
            {
                "view": "CogniteFile",
                "properties": [{"path": "name"}],
                "filters": [
                    {
                        "operator": "EQUALS",
                        "target_property": "mimeType",
                        "values": ["application/pdf"],
                    }
                ],
            }
        ],
    )
    assert "name" in paths
    assert "mimeType" in paths


def test_index_field_config_includes_filters_key() -> None:
    for view_cfg in INDEX_FIELD_CONFIG:
        assert "filters" in view_cfg
        assert isinstance(view_cfg["filters"], list)
