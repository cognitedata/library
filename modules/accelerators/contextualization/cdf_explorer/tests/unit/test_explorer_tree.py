"""Unit tests for Object Explorer tree node routing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ui.server import cdf_browse, explorer_tree


def test_parse_node_id_connection():
    kind, segs = explorer_tree.parse_node_id("connection")
    assert kind == "connection"
    assert segs == []


def test_parse_node_id_decodes_segments():
    kind, segs = explorer_tree.parse_node_id("dm:space:my%20space")
    assert kind == "dm"
    assert segs[0] == "space"
    assert segs[1] == "my space"


def test_list_children_connection_domains():
    client = MagicMock()
    with patch("ui.server.explorer_tree.cdf_browse.connection_info", return_value={"project": "test"}):
        nodes = explorer_tree.list_children(client, "connection")
    ids = {n["id"] for n in nodes}
    assert "data" in ids
    assert "raw" not in ids
    assert "dm" not in ids
    assert "classic" not in ids
    assert "tx" not in ids
    assert "orch" in ids
    assert "wf" not in ids
    assert "ep" not in ids
    assert "gov" in ids
    top_folder_ids = [n["id"] for n in nodes if n["id"] in ("data", "orch", "gov")]
    assert top_folder_ids == ["data", "orch", "gov"]


def test_list_children_orchestration_branches():
    client = MagicMock()
    nodes = explorer_tree.list_children(client, "orch")
    assert [n["id"] for n in nodes] == ["wf", "ep", "fn"]
    assert nodes[0]["label"] == "Workflows"
    assert nodes[1]["label"] == "Pipelines"
    assert nodes[2]["label"] == "Functions"


def test_list_children_functions():
    client = MagicMock()
    with patch(
        "ui.server.explorer_tree.cdf_browse.list_functions",
        return_value=[{"id": "fn-uuid-1", "label": "My Function", "external_id": "my_fn"}],
    ):
        nodes = explorer_tree.list_children(client, "fn")
    assert nodes[0]["id"] == "fn:item:fn-uuid-1"
    assert nodes[0]["kind"] == "function"


def test_list_children_data_branches():
    client = MagicMock()
    nodes = explorer_tree.list_children(client, "data")
    assert [n["id"] for n in nodes] == ["raw", "dm", "classic", "tx"]
    assert nodes[0]["label"] == "RAW"
    assert nodes[3]["label"] == "Transformations"


def test_list_children_governance_branches():
    client = MagicMock()
    nodes = explorer_tree.list_children(client, "gov")
    assert [n["id"] for n in nodes] == ["gov:spaces", "gov:groups"]
    assert nodes[0]["label"] == "Spaces"
    assert nodes[1]["label"] == "Groups"
    assert all(n["has_children"] for n in nodes)


def test_list_children_governance_spaces_and_groups():
    client = MagicMock()
    with patch(
        "ui.server.explorer_tree.cdf_browse.list_governance_spaces",
        return_value=[
            {"space": "z_space", "label": "z_space"},
            {"space": "a_space", "label": "a_space"},
        ],
    ):
        spaces = explorer_tree.list_children(client, "gov:spaces")
    assert [n["label"] for n in spaces] == ["a_space", "z_space"]
    assert spaces[0]["id"] == "gov:space:a_space"
    assert spaces[0]["kind"] == "gov_space"

    with patch(
        "ui.server.explorer_tree.cdf_browse.list_security_groups",
        return_value=[{"id": 1, "label": "Readers", "name": "Readers"}],
    ):
        groups = explorer_tree.list_children(client, "gov:groups")
    assert groups[0]["id"] == "gov:group:1"
    assert groups[0]["kind"] == "gov_group"


def test_list_children_transformations():
    client = MagicMock()
    with patch(
        "ui.server.explorer_tree.cdf_browse.list_transformations",
        return_value=[{"id": 42, "label": "My TX", "external_id": "my_tx"}],
    ):
        nodes = explorer_tree.list_children(client, "tx")
    assert nodes[0]["id"] == "tx:item:42"
    assert nodes[0]["kind"] == "transformation"


def test_list_children_classic_open_targets():
    client = MagicMock()
    nodes = explorer_tree.list_children(client, "classic")
    labels = [n["label"] for n in nodes]
    assert labels == sorted(labels, key=str.casefold)
    ids = {n["id"] for n in nodes}
    assert ids == {f"classic:{rid}" for rid, _ in cdf_browse.CLASSIC_RESOURCE_BRANCHES}
    assets = next(n for n in nodes if n["id"] == "classic:assets")
    assert assets["open_target"]["type"] == "classic_list"
    assert assets["open_target"]["resource_type"] == "assets"
    sequences = next(n for n in nodes if n["id"] == "classic:sequences")
    assert sequences["open_target"]["resource_type"] == "sequences"


def test_encode_decode_roundtrip():
    seg = "a/b:c"
    enc = explorer_tree.encode_segment(seg)
    assert explorer_tree.decode_segment(enc) == seg


def test_list_children_dm_lists_data_models_directly():
    client = MagicMock()
    with patch(
        "ui.server.explorer_tree.cdf_browse.dm_list_all_data_models",
        return_value=[
            {
                "space": "cdf_cdm",
                "external_id": "CogniteCore",
                "version": "v1",
                "name": "Cognite Core",
            }
        ],
    ):
        nodes = explorer_tree.list_children(client, "dm")
    assert len(nodes) == 1
    assert nodes[0]["id"] == "dm:model:cdf_cdm:CogniteCore:v1"
    assert "Cognite Core" in nodes[0]["label"]
    assert nodes[0]["has_children"] is True


def test_list_children_dm_model_lists_views_directly():
    client = MagicMock()
    model_id = "dm:model:cdf_cdm:CogniteCore:v1"
    view_rows = [
        {
            "space": "cdf_cdm",
            "external_id": "CogniteAsset",
            "version": "v1",
            "name": "Asset",
        }
    ]
    with patch(
        "ui.server.explorer_tree.cdf_browse.dm_list_views_for_data_model",
        return_value=view_rows,
    ):
        views = explorer_tree.list_children(client, model_id)
    assert len(views) == 1
    assert views[0]["label"] == "CogniteAsset (v1)"
    assert views[0]["has_children"] is False
    assert views[0]["open_target"]["type"] == "dm_instances"
    assert views[0]["open_target"]["view_external_id"] == "CogniteAsset"


def test_list_children_raw_databases_and_tables():
    client = MagicMock()
    with patch(
        "ui.server.explorer_tree.cdf_browse.raw_list_databases",
        return_value=["db_discovery_aliasing"],
    ):
        dbs = explorer_tree.list_children(client, "raw")
    assert len(dbs) == 1
    assert dbs[0]["id"] == "raw:db:db_discovery_aliasing"
    assert dbs[0]["has_children"] is True

    with patch(
        "ui.server.explorer_tree.cdf_browse.raw_list_tables",
        return_value=[{"name": "asset_aliases", "row_count": 10}],
    ):
        tables = explorer_tree.list_children(client, "raw:db:db_discovery_aliasing")
    assert len(tables) == 1
    assert tables[0]["id"] == "raw:db:db_discovery_aliasing:table:asset_aliases"
    assert tables[0]["open_target"]["type"] == "raw_rows"
    assert tables[0]["open_target"]["table"] == "asset_aliases"

