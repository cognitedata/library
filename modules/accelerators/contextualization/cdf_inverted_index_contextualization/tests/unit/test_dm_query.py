from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling.ids import ViewId

from inverted_index.dm_query import (
    EDGE_RESULT_KEY,
    NODE_RESULT_KEY,
    QueryStats,
    annotation_select_property_names,
    build_index_entry_filter,
    collect_view_property_paths,
    combine_edge_filter,
    combine_node_filter,
    query_all_edges,
    query_all_nodes,
    query_index_entries,
    top_level_property_names,
)


def test_top_level_property_names_dedupes_dot_paths() -> None:
    names = top_level_property_names(["name", "metadata.site", "metadata.unit", "name"])
    assert names == ["name", "metadata"]


def test_collect_view_property_paths_merges_scope() -> None:
    paths = collect_view_property_paths(
        view_external_id="CogniteFile",
        index_field_config=[
            {
                "view": "CogniteFile",
                "properties": [{"path": "description"}],
            }
        ],
        scope_config={
            "resolve_from": {"CogniteFile": {"site": ["metadata.site"]}},
            "resolve_from_default": {"unit": ["metadata.unit"]},
        },
    )
    assert "description" in paths
    assert "metadata.site" in paths
    assert "metadata.unit" in paths


def test_collect_view_property_paths_regex_candidate() -> None:
    paths = collect_view_property_paths(
        view_external_id="CogniteAsset",
        scope_config={
            "resolve_from": {
                "CogniteAsset": {
                    "site": [
                        {
                            "path": "description",
                            "extract_mode": "regex",
                            "extract_pattern": r"site:\s*(\w+)",
                        }
                    ]
                }
            }
        },
    )
    assert "description" in paths


def test_annotation_select_property_names_includes_text_and_bbox() -> None:
    names = annotation_select_property_names(
        {
            "text_property": "startNodeText",
            "bbox_properties": ["startNodeXMin", "startNodeYMin"],
            "detection_mode_property": "detectionMode",
        }
    )
    assert "startNodeText" in names
    assert "startNodeXMin" in names
    assert "detectionMode" in names
    assert "region" not in names


def test_query_all_nodes_yields_cursor_pages() -> None:
    n1 = SimpleNamespace(external_id="a1", space="sp", properties={})
    n2 = SimpleNamespace(external_id="a2", space="sp", properties={})

    page1 = MagicMock()
    page1.cursors = {NODE_RESULT_KEY: "cursor-2"}
    page1.nodes = [n1]

    page2 = MagicMock()
    page2.cursors = {}
    page2.nodes = [n2]

    client = MagicMock()
    client.data_modeling.instances.query.side_effect = [page1, page2]

    view_id = ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1")
    stats = QueryStats()
    nodes = list(
        query_all_nodes(
            client,
            view_id=view_id,
            property_names=["name"],
            instance_space="cdf_cdm",
            stats_out=stats,
        )
    )
    assert len(nodes) == 2
    assert stats.page_count == 2
    assert stats.api == "instances.query"
    assert client.data_modeling.instances.query.call_count == 2
    first_query = client.data_modeling.instances.query.call_args_list[0][0][0]
    assert first_query.with_[NODE_RESULT_KEY].filter is not None


def test_query_all_edges_passes_file_filter_in_query() -> None:
    edge = SimpleNamespace(external_id="e1", space="cdf_cdm", properties={}, start_node=None)
    page = MagicMock()
    page.cursors = {}
    page.edges = [edge]

    client = MagicMock()
    client.data_modeling.instances.query.return_value = page

    view_id = ViewId(space="cdf_cdm", external_id="CogniteDiagramAnnotation", version="v1")
    list(
        query_all_edges(
            client,
            view_id=view_id,
            property_names=["startNodeText"],
            edge_space="cdf_cdm",
            file_space="cdf_cdm",
            file_external_id="FILE_1",
            text_property="startNodeText",
        )
    )
    query_doc = client.data_modeling.instances.query.call_args[0][0]
    assert query_doc.with_[EDGE_RESULT_KEY].filter is not None


def test_query_all_nodes_requires_query_api() -> None:
    client = MagicMock()
    client.data_modeling.instances.query = None
    view_id = ViewId(space="s", external_id="V", version="v1")
    with pytest.raises(TypeError, match="instances.query is required"):
        list(query_all_nodes(client, view_id=view_id, property_names=["name"]))


def test_build_index_entry_filter_composes_terms_and_scope() -> None:
    view_id = ViewId(space="contextualization_idx", external_id="InvertedIndexEntry", version="v1")
    filt = build_index_entry_filter(
        view_id,
        normalized_terms=["p101a"],
        match_scope_key="site:RTM|unit:U100",
        source_types=["asset_metadata"],
    )
    assert filt is not None


def test_query_index_entries_maps_camel_case_properties() -> None:
    view_id = ViewId(space="contextualization_idx", external_id="InvertedIndexEntry", version="v1")
    inst = SimpleNamespace(
        external_id="iie_1",
        properties={
            view_id: {
                "term": "P-101A",
                "normalizedTerm": "p101a",
                "sourceType": "asset_metadata",
                "additionalMetadata": {"confidence": 0.9},
            }
        },
    )
    page = MagicMock()
    page.cursors = {}
    page.index_rows = [inst]

    client = MagicMock()
    client.data_modeling.instances.query.return_value = page

    rows = query_index_entries(
        client,
        view_id=view_id,
        index_space="contextualization_idx",
        normalized_terms=["p101a"],
        match_scope_key="global",
    )
    assert len(rows) == 1
    assert rows[0]["normalized_term"] == "p101a"
    assert rows[0]["source_type"] == "asset_metadata"


def test_watermark_filter_in_combine_node_filter() -> None:
    view_id = ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1")
    wm = datetime(2026, 1, 1, tzinfo=timezone.utc)
    filt = combine_node_filter(
        view_id,
        instance_space="cdf_cdm",
        watermark_filter_obj=__import__(
            "inverted_index.dm_query", fromlist=["watermark_filter"]
        ).watermark_filter("node", wm),
    )
    assert filt is not None


def test_combine_edge_filter_with_detection_mode_property() -> None:
    view_id = ViewId(space="cdf_cdm", external_id="CogniteDiagramAnnotation", version="v1")
    filt = combine_edge_filter(
        view_id,
        edge_space="cdf_cdm",
        detection_mode_property="detectionMode",
        detection_mode="pattern",
        text_property="startNodeText",
    )
    assert filt is not None
