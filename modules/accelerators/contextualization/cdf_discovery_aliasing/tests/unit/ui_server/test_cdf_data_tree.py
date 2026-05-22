"""Unit tests for workflow palette CDF Data tree routing."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_MODULE = Path(__file__).resolve().parents[3]
if str(_MODULE) not in sys.path:
    sys.path.insert(0, str(_MODULE))

from ui.server import cdf_data_tree, cdf_data_tree_browse


def test_parse_node_id_data():
    kind, segs = cdf_data_tree.parse_node_id("data")
    assert kind == "data"
    assert segs == []


def test_list_children_data_branches_only_three():
    client = MagicMock()
    nodes = cdf_data_tree.list_children(client, "data")
    assert {n["id"] for n in nodes} == {"raw", "dm", "classic"}
    assert [n["label"] for n in nodes] == sorted((n["label"] for n in nodes), key=str.casefold)
    assert "tx" not in {n["id"] for n in nodes}


def test_list_children_classic_open_targets():
    client = MagicMock()
    nodes = cdf_data_tree.list_children(client, "classic")
    assets = next(n for n in nodes if n["id"] == "classic:assets")
    assert assets["open_target"]["type"] == "classic_list"
    assert assets["open_target"]["resource_type"] == "assets"


def test_list_children_dm_lists_data_models():
    client = MagicMock()
    with patch(
        "ui.server.cdf_data_tree.cdf_data_tree_browse.dm_list_all_data_models",
        return_value=[
            {
                "space": "cdf_cdm",
                "external_id": "CogniteCore",
                "version": "v1",
                "name": "Cognite Core",
            }
        ],
    ):
        nodes = cdf_data_tree.list_children(client, "dm")
    assert len(nodes) == 1
    assert nodes[0]["id"] == "dm:model:cdf_cdm:CogniteCore:v1"


def test_list_children_raw_databases_and_tables():
    client = MagicMock()
    with patch(
        "ui.server.cdf_data_tree.cdf_data_tree_browse.raw_list_databases",
        return_value=["db_test"],
    ):
        dbs = cdf_data_tree.list_children(client, "raw")
    assert dbs[0]["id"] == "raw:db:db_test"

    with patch(
        "ui.server.cdf_data_tree.cdf_data_tree_browse.raw_list_tables",
        return_value=[{"name": "t1", "row_count": 1}],
    ):
        tables = cdf_data_tree.list_children(client, "raw:db:db_test")
    assert tables[0]["open_target"]["type"] == "raw_rows"
    assert tables[0]["open_target"]["table"] == "t1"


def test_encode_decode_roundtrip():
    seg = "a/b:c"
    enc = cdf_data_tree.encode_segment(seg)
    assert cdf_data_tree.decode_segment(enc) == seg


def test_sort_nodes_stars_before_alpha():
    nodes = [
        {"id": "node-m", "label": "Middle", "kind": "x", "has_children": False},
        {"id": "node-z", "label": "Zulu", "kind": "x", "has_children": False},
        {"id": "node-a", "label": "Alpha", "kind": "x", "has_children": False},
    ]
    out = cdf_data_tree._sort_nodes(nodes, starred_ids=["node-z", "node-a"])
    assert [n["id"] for n in out] == ["node-z", "node-a", "node-m"]
    assert out[0]["starred"] is True
    assert out[1]["starred"] is True
