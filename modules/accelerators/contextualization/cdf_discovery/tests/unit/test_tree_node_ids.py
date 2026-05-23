"""Unit tests for Discovery tree node id constants and starred config."""

from __future__ import annotations

from pathlib import Path

import yaml

from ui.server import discovery_config
from ui.server.tree_node_ids import DATA_SAVED_QUERIES, FUSION_DM_ROOT, FUSION_INTEGRATION_ROOT


def test_get_starred_node_ids_dedupes_without_legacy_migration(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(discovery_config, "DEFAULT_CONFIG_PATH", tmp_path / "default.config.yaml")
    monkeypatch.setattr(discovery_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.local.config.yaml")
    (tmp_path / "default.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}}), encoding="utf-8"
    )
    raw = [
        "data:sq",
        "data:sq:item:q1",
        "fusion:integration:transformations:item:9",
        "fusion:integration:transformations:item:9",
    ]
    (tmp_path / "discovery.local.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": raw}}),
        encoding="utf-8",
    )

    starred = discovery_config.get_starred_node_ids()
    assert starred == [
        "data:sq",
        "data:sq:item:q1",
        "fusion:integration:transformations:item:9",
    ]

    local = yaml.safe_load((tmp_path / "discovery.local.config.yaml").read_text(encoding="utf-8"))
    assert local["stars"]["node_ids"] == raw


def test_set_starred_node_ids_normalizes_only():
    assert DATA_SAVED_QUERIES == "data:sq"
    assert FUSION_DM_ROOT == "fusion:dm"
    assert FUSION_INTEGRATION_ROOT == "fusion:integration"
