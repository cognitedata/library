"""Unit tests for explorer operator config (stars)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ui.server import explorer_config, explorer_tree


def test_normalize_node_ids_dedupes_and_preserves_order():
    raw = ["b", "a", "b", "", "  a  "]
    assert explorer_config._normalize_node_ids(raw) == ["b", "a"]


def test_sort_nodes_stars_before_alpha(tmp_path, monkeypatch):
    monkeypatch.setattr(explorer_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(explorer_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(explorer_config, "LOCAL_CONFIG_PATH", tmp_path / "explorer.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": ["node-z", "node-a"]}}), encoding="utf-8"
    )

    nodes = [
        {"id": "node-m", "label": "Middle", "kind": "x", "has_children": False},
        {"id": "node-z", "label": "Zulu", "kind": "x", "has_children": False},
        {"id": "node-a", "label": "Alpha", "kind": "x", "has_children": False},
    ]
    out = explorer_tree._sort_nodes(nodes)
    assert [n["id"] for n in out] == ["node-z", "node-a", "node-m"]
    assert out[0]["starred"] is True
    assert out[1]["starred"] is True
    assert "starred" not in out[2] or out[2].get("starred") is not True


def test_set_starred_node_ids_writes_local_config(tmp_path, monkeypatch):
    monkeypatch.setattr(explorer_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(explorer_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(explorer_config, "LOCAL_CONFIG_PATH", tmp_path / "explorer.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}}), encoding="utf-8"
    )

    saved = explorer_config.set_starred_node_ids(["tx:item:1", "dm:model:a:b:v1"])
    assert saved == ["tx:item:1", "dm:model:a:b:v1"]
    assert explorer_config.get_starred_node_ids() == saved
    local = yaml.safe_load((tmp_path / "explorer.local.config.yaml").read_text(encoding="utf-8"))
    assert local["stars"]["node_ids"] == saved


def test_get_workspace_empty_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr(explorer_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(explorer_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(explorer_config, "LOCAL_CONFIG_PATH", tmp_path / "explorer.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}, "workspace": {"active_tab_id": None, "tabs": []}}),
        encoding="utf-8",
    )
    assert explorer_config.get_workspace() == {"active_tab_id": None, "tabs": []}


def test_set_workspace_persists_and_normalizes(tmp_path, monkeypatch):
    monkeypatch.setattr(explorer_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(explorer_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(explorer_config, "LOCAL_CONFIG_PATH", tmp_path / "explorer.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}, "workspace": {"active_tab_id": None, "tabs": []}}),
        encoding="utf-8",
    )

    saved = explorer_config.set_workspace(
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
    assert explorer_config.get_workspace() == saved
    assert explorer_config.public_config()["workspace"] == saved

    stars = explorer_config.set_starred_node_ids(["node-a"])
    assert stars == ["node-a"]
    assert explorer_config.get_workspace() == saved


def test_set_workspace_replaces_removed_tabs(tmp_path, monkeypatch):
    monkeypatch.setattr(explorer_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(explorer_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(explorer_config, "LOCAL_CONFIG_PATH", tmp_path / "explorer.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}, "workspace": {"active_tab_id": None, "tabs": []}}),
        encoding="utf-8",
    )

    explorer_config.set_workspace(
        {
            "active_tab_id": "sql:a",
            "tabs": [
                {"kind": "sql", "id": "sql:a", "query": "SELECT 1"},
                {"kind": "sql", "id": "sql:b", "query": "SELECT 2"},
            ],
        }
    )
    saved = explorer_config.set_workspace(
        {"active_tab_id": "sql:a", "tabs": [{"kind": "sql", "id": "sql:a", "query": "SELECT 1"}]}
    )
    assert saved == {
        "active_tab_id": "sql:a",
        "tabs": [{"kind": "sql", "id": "sql:a", "query": "SELECT 1"}],
    }
    assert explorer_config.get_workspace() == saved


def test_set_saved_queries_persists(tmp_path, monkeypatch):
    monkeypatch.setattr(explorer_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(explorer_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(explorer_config, "LOCAL_CONFIG_PATH", tmp_path / "explorer.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"saved_queries": {"queries": []}}), encoding="utf-8"
    )

    saved = explorer_config.set_saved_queries(
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
    assert explorer_config.get_saved_queries() == saved
