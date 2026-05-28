"""Tests for manual governance Space/Group artifact creation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ui.server import governance_declared


def test_create_space_artifact_file(tmp_path: Path):
    mod = tmp_path / "mod"
    mod.mkdir()
    (mod / "scripts").mkdir()
    declared = mod / "governance"
    declared.mkdir()
    cfg = declared / "default.config.yaml"
    cfg.write_text("scope_hierarchy:\n  type: hierarchy\n", encoding="utf-8")

    out = governance_declared.create_artifact_file(
        declared=declared,
        config_path=cfg,
        discovery_module_root=mod,
        kind="spaces",
        external_id="inst_site_a_asset",
        display_name="Site A Asset",
        parent_rel="spaces/site_a",
    )
    rel = out["path"]
    assert rel.endswith(".Space.yaml")
    assert rel.startswith("spaces/site_a/")
    path = mod / rel
    assert path.is_file()
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert doc["space"] == "inst_site_a_asset"
    assert doc["name"] == "Site A Asset"


def test_create_group_artifact_file(tmp_path: Path):
    mod = tmp_path / "mod"
    mod.mkdir()
    declared = mod / "governance"
    declared.mkdir()
    cfg = declared / "default.config.yaml"
    cfg.write_text("groups:\n  global:\n    source_ids: {}\n", encoding="utf-8")
    scripts = mod / "scripts"
    scripts.mkdir()
    (scripts / "governance_build").mkdir()
    (scripts / "governance_build" / "__init__.py").write_text("", encoding="utf-8")
    (scripts / "governance_build" / "toolkit_sync.py").write_text(
        "def upsert_group_source_id_from_group_yaml(**kwargs):\n    return False\n",
        encoding="utf-8",
    )

    out = governance_declared.create_artifact_file(
        declared=declared,
        config_path=cfg,
        discovery_module_root=mod,
        kind="groups",
        external_id="gp_asset_site_a_read",
        parent_rel="auth",
    )
    rel = out["path"]
    assert rel.endswith(".Group.yaml")
    doc = yaml.safe_load((mod / rel).read_text(encoding="utf-8"))
    assert doc["name"] == "gp_asset_site_a_read"
    assert "capabilities" in doc


def test_create_rejects_duplicate(tmp_path: Path):
    mod = tmp_path / "mod"
    mod.mkdir()
    (mod / "scripts").mkdir()
    declared = mod / "governance"
    declared.mkdir()
    sp = mod / "spaces"
    sp.mkdir(parents=True)
    (sp / "foo.Space.yaml").write_text("space: inst_x\nname: x\n", encoding="utf-8")
    cfg = declared / "default.config.yaml"
    cfg.write_text("scope_hierarchy:\n  type: hierarchy\n", encoding="utf-8")

    with pytest.raises(ValueError, match="already exists"):
        governance_declared.create_artifact_file(
            declared=declared,
            config_path=cfg,
            discovery_module_root=mod,
            kind="spaces",
            external_id="inst_x",
            display_name="Foo",
            parent_rel="spaces",
        )
