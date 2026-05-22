"""Unit tests for workflow palette operator config (stars)."""

from __future__ import annotations

from pathlib import Path

import yaml

from ui.server import palette_operator_config


def test_normalize_node_ids_dedupes_and_preserves_order():
    raw = ["b", "a", "b", "", "  a  "]
    assert palette_operator_config._normalize_node_ids(raw) == ["b", "a"]


def test_set_starred_node_ids_writes_local_config(tmp_path, monkeypatch):
    monkeypatch.setattr(palette_operator_config, "MODULE_ROOT", tmp_path)
    monkeypatch.setattr(palette_operator_config, "DEFAULT_CONFIG_PATH", tmp_path / "operator.config.yaml")
    monkeypatch.setattr(
        palette_operator_config, "LOCAL_CONFIG_PATH", tmp_path / "discovery.operator.local.config.yaml"
    )
    (tmp_path / "operator.config.yaml").write_text(
        yaml.safe_dump({"stars": {"node_ids": []}}), encoding="utf-8"
    )

    saved = palette_operator_config.set_starred_node_ids(
        ["dm:model:cdf_cdm:CogniteCore:v1", "classic:assets"]
    )
    assert saved == ["dm:model:cdf_cdm:CogniteCore:v1", "classic:assets"]
    assert palette_operator_config.get_starred_node_ids() == saved
    local = yaml.safe_load(
        (tmp_path / "discovery.operator.local.config.yaml").read_text(encoding="utf-8")
    )
    assert local["stars"]["node_ids"] == saved
