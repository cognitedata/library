"""Unit tests for Discovery object tree node routing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ui.server import cdf_browse, discovery_tree


def test_parse_node_id_connection():
    kind, segs = discovery_tree.parse_node_id("connection")
    assert kind == "connection"
    assert segs == []


def test_parse_node_id_decodes_segments():
    kind, segs = discovery_tree.parse_node_id("dm:space:my%20space")
    assert kind == "dm"
    assert segs[0] == "space"
    assert segs[1] == "my space"


def test_list_children_connection_domains():
    client = MagicMock()
    with patch("ui.server.discovery_tree.cdf_browse.connection_info", return_value={"project": "test"}):
        nodes = discovery_tree.list_children(client, "connection")
    ids = {n["id"] for n in nodes}
    assert "data" in ids
    assert "raw" not in ids
    assert "dm" not in ids
    assert "classic" not in ids
    assert "tx" not in ids
    assert "integration" not in ids
    assert "orch" not in ids
    assert "wf" not in ids
    assert "ep" not in ids
    assert "gov" in ids
    assert "fusion" in ids
    top_folder_ids = [n["id"] for n in nodes if n["id"] in ("data", "fusion", "gov")]
    assert top_folder_ids == ["data", "fusion", "gov"]


def test_list_children_fusion_integration_branches():
    client = MagicMock()
    nodes = discovery_tree.list_children(client, "fusion:integration")
    assert [n["id"] for n in nodes] == [
        "fusion:integration:workflows",
        "fusion:integration:pipelines",
        "fusion:integration:functions",
        "fusion:integration:transformations",
    ]
    assert nodes[0]["label"] == "Workflows"
    assert nodes[1]["label"] == "Pipelines"
    assert nodes[2]["label"] == "Functions"
    assert nodes[3]["label"] == "Transformations"


def test_fusion_integration_folder_under_fusion_root():
    client = MagicMock()
    with patch("ui.server.discovery_tree.cdf_browse.connection_info", return_value={"project": "test"}):
        fusion_children = discovery_tree.list_children(client, "fusion")
    integration = next(n for n in fusion_children if n["id"] == "fusion:integration")
    assert integration["label"] == "Integration"
    assert integration["meta"]["domain"] == "integration"


def test_list_children_functions():
    client = MagicMock()
    with patch(
        "ui.server.discovery_tree.cdf_browse.list_functions",
        return_value=[{"id": "fn-uuid-1", "label": "My Function", "external_id": "my_fn"}],
    ):
        nodes = discovery_tree.list_children(client, "fusion:integration:functions")
    assert nodes[0]["id"] == "fusion:integration:functions:item:fn-uuid-1"
    assert nodes[0]["kind"] == "function"


def test_list_children_data_branches():
    client = MagicMock()
    nodes = discovery_tree.list_children(client, "data")
    assert [n["id"] for n in nodes] == ["data:sq", "raw", "dm", "classic"]
    assert nodes[0]["label"] == "Saved Queries"
    assert nodes[1]["label"] == "RAW"
    assert nodes[2]["label"] == "Data Models"
    assert nodes[3]["label"] == "Classic"


def test_list_children_fusion_at_connection_root():
    client = MagicMock()
    with patch("ui.server.discovery_tree.cdf_browse.connection_info", return_value={"project": "test"}):
        nodes = discovery_tree.list_children(client, "connection")
    fusion = next(n for n in nodes if n["id"] == "fusion")
    assert fusion["label"] == "Fusion"
    assert fusion["meta"]["domain"] == "fusion"
    assert fusion["has_children"] is True


def test_list_children_saved_queries_under_data():
    client = MagicMock()
    with patch(
        "ui.server.discovery_tree.discovery_config.get_saved_queries",
        return_value=[
            {
                "id": "my_assets",
                "name": "My assets",
                "query": "SELECT 1",
                "limit": 100,
                "convert_to_string": True,
            }
        ],
    ):
        nodes = discovery_tree.list_children(client, "data:sq")
    assert len(nodes) == 1
    assert nodes[0]["id"] == "data:sq:item:my_assets"
    assert nodes[0]["kind"] == "saved_query"


def test_list_children_governance_branches():
    client = MagicMock()
    nodes = discovery_tree.list_children(client, "gov")
    assert [n["id"] for n in nodes] == ["gov:spaces", "gov:groups"]
    assert nodes[0]["label"] == "Spaces"
    assert nodes[0]["meta"]["governance_workspace"] == "spaces"
    assert nodes[1]["label"] == "Groups"
    assert nodes[1]["meta"]["governance_workspace"] == "groups"
    assert all(n["has_children"] for n in nodes)


def test_gov_workspace_children_live_before_artifacts():
    client = MagicMock()
    with (
        patch(
            "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
            return_value=[{"space": "dm_src_acme", "label": "dm_src_acme"}],
        ),
        patch(
            "ui.server.discovery_tree._gov_artifact_branch_nodes",
            return_value=[
                {
                    "id": "gov:spaces:adir:spaces",
                    "label": "artifacts",
                    "kind": "folder",
                    "has_children": True,
                }
            ],
        ),
    ):
        nodes = discovery_tree.list_children(client, "gov:spaces")
    assert nodes[0]["kind"] == "folder"
    assert nodes[0]["label"] == "dm"
    assert nodes[0]["id"] == "gov:spaces:live:dm"
    assert nodes[1]["kind"] == "folder"

    with patch(
        "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
        return_value=[{"space": "dm_src_acme", "label": "dm_src_acme"}],
    ):
        under_dm = discovery_tree.list_children(client, "gov:spaces:live:dm")
    assert [n["label"] for n in under_dm] == ["src"]
    assert under_dm[0]["kind"] == "folder"

    with patch(
        "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
        return_value=[{"space": "dm_src_acme", "label": "dm_src_acme"}],
    ):
        leaves = discovery_tree.list_children(client, "gov:spaces:live:dm:src")
    assert leaves[0]["label"] == "dm_src_acme"
    assert leaves[0]["kind"] == "gov_space"


def test_list_children_governance_spaces_and_groups():
    client = MagicMock()
    with (
        patch(
            "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
            return_value=[
                {"space": "z_space_extra", "label": "z_space_extra"},
                {"space": "a_space_extra", "label": "a_space_extra"},
                {"space": "a_other_site", "label": "a_other_site"},
            ],
        ),
        patch(
            "ui.server.discovery_tree._gov_artifact_branch_nodes",
            return_value=[],
        ),
    ):
        spaces = discovery_tree.list_children(client, "gov:spaces")
    assert [n["label"] for n in spaces] == ["a", "z"]
    assert spaces[0]["id"] == "gov:spaces:live:a"
    assert spaces[0]["kind"] == "folder"
    assert spaces[0]["has_children"] is True

    with patch(
        "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
        return_value=[
            {"space": "z_space_extra", "label": "z_space_extra"},
            {"space": "a_space_extra", "label": "a_space_extra"},
            {"space": "a_other_site", "label": "a_other_site"},
        ],
    ):
        under_a = discovery_tree.list_children(client, "gov:spaces:live:a")
    assert [n["label"] for n in under_a] == ["other", "space"]

    with patch(
        "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
        return_value=[{"space": "a_space_extra", "label": "a_space_extra"}],
    ):
        leaves = discovery_tree.list_children(client, "gov:spaces:live:a:space")
    assert [n["label"] for n in leaves] == ["a_space_extra"]
    assert leaves[0]["id"] == "gov:space:a_space_extra"
    assert leaves[0]["kind"] == "gov_space"

    with patch(
        "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
        return_value=[
            {"space": "a_space_extra", "label": "a_space_extra"},
            {"space": "a_space_other", "label": "a_space_other"},
        ],
    ):
        under_a_space = discovery_tree.list_children(client, "gov:spaces:live:a:space")
    assert {n["label"] for n in under_a_space} == {"a_space_extra", "a_space_other"}

    with (
        patch(
            "ui.server.discovery_tree.cdf_browse.list_security_groups",
            return_value=[{"id": 1, "label": "Readers", "name": "gp_asset_site_a_read"}],
        ),
        patch(
            "ui.server.discovery_tree._gov_artifact_branch_nodes",
            return_value=[],
        ),
    ):
        groups = discovery_tree.list_children(client, "gov:groups")
    assert groups[0]["id"] == "gov:groups:live:gp"
    assert groups[0]["kind"] == "folder"

    with patch(
        "ui.server.discovery_tree.cdf_browse.list_security_groups",
        return_value=[{"id": 1, "label": "Readers", "name": "gp_asset_site_a_read"}],
    ):
        group_leaves = discovery_tree.list_children(client, "gov:groups:live:gp:asset")
    assert group_leaves[0]["id"] == "gov:group:1"
    assert group_leaves[0]["kind"] == "gov_group"


def test_single_token_governance_names_are_root_leaves():
    client = MagicMock()
    with (
        patch(
            "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
            return_value=[
                {"space": "global", "label": "global"},
                {"space": "dm_src_acme", "label": "dm_src_acme"},
            ],
        ),
        patch(
            "ui.server.discovery_tree._gov_artifact_branch_nodes",
            return_value=[],
        ),
    ):
        nodes = discovery_tree.list_children(client, "gov:spaces")
    assert nodes[0]["label"] == "dm"
    assert nodes[0]["kind"] == "folder"
    assert nodes[1]["label"] == "global"
    assert nodes[1]["kind"] == "gov_space"
    assert nodes[1]["id"] == "gov:space:global"


def test_governance_live_tree_respects_configured_token_depth():
    client = MagicMock()
    with (
        patch(
            "ui.server.discovery_tree.discovery_config.get_gov_live_token_folder_depth",
            return_value=1,
        ),
        patch(
            "ui.server.discovery_tree.cdf_browse.list_governance_spaces",
            return_value=[{"space": "dm_src_acme", "label": "dm_src_acme"}],
        ),
        patch(
            "ui.server.discovery_tree._gov_artifact_branch_nodes",
            return_value=[],
        ),
    ):
        under_dm = discovery_tree.list_children(client, "gov:spaces:live:dm")
    assert [n["label"] for n in under_dm] == ["dm_src_acme"]
    assert under_dm[0]["kind"] == "gov_space"


def test_name_tokens():
    assert discovery_tree._name_tokens("dm_src_acme_factory") == ["dm", "src", "acme", "factory"]
    assert discovery_tree._name_tokens("am-long-europe-dev") == ["am", "long", "europe", "dev"]
    assert discovery_tree._name_tokens("gp:cdf:valhall:read") == ["gp", "cdf", "valhall", "read"]
    assert discovery_tree._name_tokens("dm-src:acme_site") == ["dm", "src", "acme", "site"]
    assert discovery_tree._name_tokens("global") == ["global"]
    assert discovery_tree._name_tokens("") == ["(other)"]


def test_list_children_transformations():
    client = MagicMock()
    with patch(
        "ui.server.discovery_tree.cdf_browse.list_transformations",
        return_value=[{"id": 42, "label": "My TX", "external_id": "my_tx"}],
    ):
        nodes = discovery_tree.list_children(client, "fusion:integration:transformations")
    assert nodes[0]["id"] == "fusion:integration:transformations:item:42"
    assert nodes[0]["kind"] == "transformation"


def test_list_children_classic_open_targets():
    client = MagicMock()
    nodes = discovery_tree.list_children(client, "classic")
    labels = [n["label"] for n in nodes]
    assert labels == sorted(labels, key=str.casefold)
    ids = {n["id"] for n in nodes}
    assert ids == {f"classic:{rid}" for rid, _ in cdf_browse.CLASSIC_RESOURCE_BRANCHES}
    assets = next(n for n in nodes if n["id"] == "classic:assets")
    assert assets["open_target"]["type"] == "classic_list"
    assert assets["open_target"]["resource_type"] == "assets"
    sequences = next(n for n in nodes if n["id"] == "classic:sequences")
    assert sequences["open_target"]["resource_type"] == "sequences"


def test_list_children_fusion_root_branches():
    client = MagicMock()
    nodes = discovery_tree.list_children(client, "fusion")
    assert [n["id"] for n in nodes] == ["fusion:dm", "fusion:integration"]
    assert nodes[0]["label"] == "Data Modeling"
    assert nodes[1]["label"] == "Integration"


def test_list_children_fusion_dm_root_branches():
    client = MagicMock()
    nodes = discovery_tree.list_children(client, "fusion:dm")
    assert [n["id"] for n in nodes] == [
        "fusion:dm:nodes",
        "fusion:dm:edges",
        "fusion:dm:system",
        "fusion:dm:spaces",
    ]
    assert nodes[0]["open_target"] == {"type": "fusion_dm_all", "entity": "nodes"}
    assert nodes[1]["open_target"] == {"type": "fusion_dm_all", "entity": "edges"}
    assert nodes[2]["has_children"] is True
    assert nodes[3]["has_children"] is True
    assert nodes[0]["open_target"] == {"type": "fusion_dm_all", "entity": "nodes"}
    assert nodes[1]["open_target"] == {"type": "fusion_dm_all", "entity": "edges"}
    assert nodes[2]["has_children"] is True
    assert nodes[3]["has_children"] is True


def test_list_children_fusion_space_views_and_containers():
    client = MagicMock()
    view_rows = [
        {
            "space": "cdf_cdm",
            "external_id": "CogniteAsset",
            "version": "v1",
            "label": "CogniteAsset (v1)",
        }
    ]
    container_rows = [
        {
            "space": "cdf_cdm",
            "external_id": "CogniteAsset",
            "label": "CogniteAsset",
        }
    ]
    with (
        patch(
            "ui.server.discovery_tree.cdf_browse.fusion_list_views_in_space",
            return_value=view_rows,
        ),
        patch(
            "ui.server.discovery_tree.cdf_browse.fusion_list_containers_in_space",
            return_value=container_rows,
        ),
        patch(
            "ui.server.discovery_tree.cdf_browse.fusion_view_by_container_lookup",
            return_value=cdf_browse.fusion_view_by_container_lookup(view_rows),
        ),
    ):
        views = discovery_tree.list_children(client, "fusion:dm:space:cdf_cdm:views")
        containers = discovery_tree.list_children(
            client, "fusion:dm:space:cdf_cdm:containers"
        )
    assert views[0]["kind"] == "fusion_dm_view"
    assert views[0]["open_target"]["type"] == "dm_instances"
    assert containers[0]["kind"] == "fusion_dm_container"
    assert containers[0]["open_target"]["view_external_id"] == "CogniteAsset"


def test_list_children_fusion_space_models_open_flow_viewer():
    client = MagicMock()
    model_rows = [
        {
            "space": "cdf_cdm",
            "external_id": "CogniteCore",
            "version": "v1",
            "name": "Cognite Core",
        }
    ]
    with patch(
        "ui.server.discovery_tree.cdf_browse.dm_list_data_models",
        return_value=model_rows,
    ):
        models = discovery_tree.list_children(client, "fusion:dm:space:cdf_cdm:models")
    assert len(models) == 1
    assert models[0]["kind"] == "dm_data_model"
    assert models[0]["id"] == "fusion:dm:space:cdf_cdm:model:CogniteCore:v1"
    assert models[0]["has_children"] is True


def test_list_children_fusion_model_lists_views_like_data_branch():
    client = MagicMock()
    model_id = "fusion:dm:space:cdf_cdm:model:CogniteCore:v1"
    view_rows = [
        {
            "space": "cdf_cdm",
            "external_id": "CogniteAsset",
            "version": "v1",
            "name": "Asset",
        }
    ]
    with patch(
        "ui.server.discovery_tree.cdf_browse.dm_list_views_for_data_model",
        return_value=view_rows,
    ):
        views = discovery_tree.list_children(client, model_id)
    assert len(views) == 1
    assert views[0]["kind"] == "dm_view"
    assert views[0]["open_target"]["type"] == "dm_instances"
    assert views[0]["open_target"]["view_external_id"] == "CogniteAsset"


def test_encode_decode_roundtrip():
    seg = "a/b:c"
    enc = discovery_tree.encode_segment(seg)
    assert discovery_tree.decode_segment(enc) == seg


def test_list_children_dm_lists_data_models_directly():
    client = MagicMock()
    with patch(
        "ui.server.discovery_tree.cdf_browse.dm_list_all_data_models",
        return_value=[
            {
                "space": "cdf_cdm",
                "external_id": "CogniteCore",
                "version": "v1",
                "name": "Cognite Core",
            }
        ],
    ):
        nodes = discovery_tree.list_children(client, "dm")
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
        "ui.server.discovery_tree.cdf_browse.dm_list_views_for_data_model",
        return_value=view_rows,
    ):
        views = discovery_tree.list_children(client, model_id)
    assert len(views) == 1
    assert views[0]["label"] == "CogniteAsset (v1)"
    assert views[0]["has_children"] is False
    assert views[0]["open_target"]["type"] == "dm_instances"
    assert views[0]["open_target"]["view_external_id"] == "CogniteAsset"


def test_list_children_raw_databases_and_tables():
    client = MagicMock()
    with patch(
        "ui.server.discovery_tree.cdf_browse.raw_list_databases",
        return_value=["db_discovery_aliasing"],
    ):
        dbs = discovery_tree.list_children(client, "raw")
    assert len(dbs) == 1
    assert dbs[0]["id"] == "raw:db:db_discovery_aliasing"
    assert dbs[0]["has_children"] is True

    with patch(
        "ui.server.discovery_tree.cdf_browse.raw_list_tables",
        return_value=[{"name": "asset_aliases", "row_count": 10}],
    ):
        tables = discovery_tree.list_children(client, "raw:db:db_discovery_aliasing")
    assert len(tables) == 1
    assert tables[0]["id"] == "raw:db:db_discovery_aliasing:table:asset_aliases"
    assert tables[0]["open_target"]["type"] == "raw_rows"
    assert tables[0]["open_target"]["table"] == "asset_aliases"

