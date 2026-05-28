"""Unit tests for discovery operator config (stars)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ui.server import discovery_config, discovery_tree


def test_normalize_node_ids_dedupes_and_preserves_order():
    raw = ["b", "a", "b", "", "  a  "]
    assert discovery_config._normalize_node_ids(raw) == ["b", "a"]


def test_sort_nodes_stars_before_alpha(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": ["node-z", "node-a"]}}), encoding="utf-8"
    )

    nodes = [
        {"id": "node-m", "label": "Middle", "kind": "x", "has_children": False},
        {"id": "node-z", "label": "Zulu", "kind": "x", "has_children": False},
        {"id": "node-a", "label": "Alpha", "kind": "x", "has_children": False},
    ]
    out = discovery_tree._sort_nodes(nodes)
    assert [n["id"] for n in out] == ["node-z", "node-a", "node-m"]
    assert out[0]["starred"] is True
    assert out[1]["starred"] is True
    assert "starred" not in out[2] or out[2].get("starred") is not True


def test_set_starred_node_ids_writes_local_config(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}}), encoding="utf-8"
    )

    saved = discovery_config.set_starred_node_ids(["tx:item:1", "dm:model:a:b:v1"])
    assert saved == ["tx:item:1", "dm:model:a:b:v1"]
    assert discovery_config.get_starred_node_ids() == saved
    local = yaml.safe_load((tmp_path / "discovery.local.config.yaml").read_text(encoding="utf-8"))
    assert local["stars"]["node_ids"] == saved


def test_get_workspace_empty_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}, "workspace": {"active_tab_id": None, "tabs": []}}),
        encoding="utf-8",
    )
    assert discovery_config.get_workspace() == {"active_tab_id": None, "tabs": []}


def test_set_workspace_persists_and_normalizes(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}, "workspace": {"active_tab_id": None, "tabs": []}}),
        encoding="utf-8",
    )

    saved = discovery_config.set_workspace(
        {
            "active_tab_id": "missing",
            "tabs": [
                {
                    "kind": "sql",
                    "id": "sql:workspace",
                    "query": "SELECT 1",
                    "limit": 50,
                },
                {"kind": "workflow", "id": "wf:demo", "external_id": "demo"},
                {"kind": "sql", "id": "sql:workspace", "query": "dup"},
            ],
        }
    )
    assert saved["active_tab_id"] == "sql:workspace"
    assert len(saved["tabs"]) == 2
    assert saved["tabs"][0]["kind"] == "sql"
    assert discovery_config.get_workspace() == saved
    assert discovery_config.public_config()["workspace"] == saved

    stars = discovery_config.set_starred_node_ids(["node-a"])
    assert stars == ["node-a"]
    assert discovery_config.get_workspace() == saved


def test_set_workspace_replaces_removed_tabs(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}, "workspace": {"active_tab_id": None, "tabs": []}}),
        encoding="utf-8",
    )

    discovery_config.set_workspace(
        {
            "active_tab_id": "sql:a",
            "tabs": [
                {"kind": "sql", "id": "sql:a", "query": "SELECT 1"},
                {"kind": "sql", "id": "sql:b", "query": "SELECT 2"},
            ],
        }
    )
    saved = discovery_config.set_workspace(
        {"active_tab_id": "sql:a", "tabs": [{"kind": "sql", "id": "sql:a", "query": "SELECT 1"}]}
    )
    assert saved == {
        "active_tab_id": "sql:a",
        "tabs": [{"kind": "sql", "id": "sql:a", "query": "SELECT 1"}],
    }
    assert discovery_config.get_workspace() == saved


def test_set_workspace_normalizes_all_tab_kinds(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}, "workspace": {"active_tab_id": None, "tabs": []}}),
        encoding="utf-8",
    )

    saved = discovery_config.set_workspace(
        {
            "active_tab_id": "etl:pipeline:my-pipe",
            "tabs": [
                {"kind": "governance_spaces", "id": "gov:spaces", "active_sub_tab": "artifacts"},
                {"kind": "governance_cdf_group", "id": "gov:group:1", "group_id": 42},
                {"kind": "etl_pipeline", "id": "etl:pipeline:my-pipe", "pipeline_id": "my-pipe"},
                {"kind": "etl_template", "id": "etl:template:tpl-a", "template_id": "tpl-a"},
                {
                    "kind": "sql",
                    "id": "sql:file:parquet:99",
                    "query": "SELECT * FROM data",
                    "engine": "file_content",
                    "file_content": {"format": "parquet", "file_id": 99},
                },
            ],
        }
    )
    assert saved["active_tab_id"] == "etl:pipeline:my-pipe"
    kinds = [t["kind"] for t in saved["tabs"]]
    assert kinds == [
        "governance_spaces",
        "governance_cdf_group",
        "etl_pipeline",
        "etl_template",
        "sql",
    ]
    assert saved["tabs"][0]["active_sub_tab"] == "artifacts"
    assert saved["tabs"][4]["engine"] == "file_content"
    assert discovery_config.get_workspace() == saved


def test_set_saved_queries_persists(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"saved_queries": {"queries": []}}), encoding="utf-8"
    )

    saved = discovery_config.set_saved_queries(
        [
            {
                "id": "my_query",
                "name": "My Query",
                "query": "SELECT 1",
                "limit": 50,
            },
            {"id": "bad id", "name": "X", "query": "SELECT 2"},
        ]
    )
    assert len(saved) == 1
    assert saved[0]["id"] == "my_query"
    assert discovery_config.get_saved_queries() == saved


def test_get_gov_live_token_folder_depth_defaults_to_two(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"governance": {"live_tree_token_depth": 2}}), encoding="utf-8"
    )
    assert discovery_config.get_gov_live_token_folder_depth() == 2
    assert discovery_config.public_config()["governance"] == {"live_tree_token_depth": 2}


def test_get_gov_live_token_folder_depth_local_override(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"governance": {"live_tree_token_depth": 2}}), encoding="utf-8"
    )
    (tmp_path / "discovery.local.config.yaml").write_text(
        yaml.safe_dump({"governance": {"live_tree_token_depth": 1}}), encoding="utf-8"
    )
    assert discovery_config.get_gov_live_token_folder_depth() == 1


def test_normalize_gov_live_token_folder_depth_clamps():
    assert discovery_config._normalize_gov_live_token_folder_depth(-1) == 0
    assert discovery_config._normalize_gov_live_token_folder_depth(99) == discovery_config._MAX_GOV_LIVE_TOKEN_FOLDER_DEPTH
    assert discovery_config._normalize_gov_live_token_folder_depth("bad") == discovery_config._DEFAULT_GOV_LIVE_TOKEN_FOLDER_DEPTH
