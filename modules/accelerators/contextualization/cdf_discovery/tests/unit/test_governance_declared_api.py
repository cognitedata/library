"""Declared governance API helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from ui.server import governance_declared


def test_declared_root_env(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("CDF_DISCOVERY_GOVERNANCE_ROOT", str(tmp_path))
    assert governance_declared.declared_root(Path("/unused/discovery")) == tmp_path.resolve()


def test_default_declared_root_under_discovery_module(tmp_path: Path):
    mod = tmp_path / "cdf_discovery"
    mod.mkdir()
    expected = (mod / "governance").resolve()
    assert governance_declared.default_declared_root(mod) == expected


def test_declared_root_defaults_to_discovery_governance(tmp_path: Path):
    mod = tmp_path / "cdf_discovery"
    mod.mkdir()
    assert governance_declared.declared_root(mod) == (mod / "governance").resolve()


def test_list_artifact_paths_empty(tmp_path: Path):
    assert governance_declared.list_artifact_paths(tmp_path, "spaces") == []


def test_list_artifact_tree_children(tmp_path: Path):
    sp = tmp_path / "spaces" / "site_a"
    sp.mkdir(parents=True)
    f = sp / "foo.Space.yaml"
    f.write_text("space: x\n", encoding="utf-8")
    children = governance_declared.list_artifact_tree_children(tmp_path, kind="spaces", prefix="")
    assert len(children) >= 1
    assert children[0].get("kind") in ("file", "dir")


def test_governance_config_model_route(monkeypatch, tmp_path: Path):
    declared = tmp_path / "declared"
    declared.mkdir()
    (declared / "default.config.yaml").write_text(
        "scope_hierarchy:\n  type: hierarchy\n  levels: [site]\n  locations: []\n"
        "dimensions:\n  app:\n    type: list\n    order: 1\n    items: []\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CDF_DISCOVERY_GOVERNANCE_ROOT", str(declared))
    from fastapi.testclient import TestClient

    from ui.server.main import app

    client = TestClient(app)
    r = client.get("/api/governance/declared/config/model")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("scope_hierarchy", {}).get("type") == "hierarchy"
    assert "app" in (body.get("dimensions") or {})


def test_run_build_command_assembly(monkeypatch, tmp_path: Path):
    module_root = tmp_path / "discovery_mod"
    module_root.mkdir()
    scripts = module_root / "scripts"
    scripts.mkdir()
    declared = tmp_path / "declared"
    declared.mkdir()
    cfg = declared / "default.config.yaml"
    cfg.write_text("scope_hierarchy:\n  type: hierarchy\n", encoding="utf-8")

    captured: list = []

    def fake_run(cmd, **kwargs):
        captured.append(cmd)
        class P:
            returncode = 0
            stdout = ""
            stderr = ""

        return P()

    monkeypatch.setattr(governance_declared.subprocess, "run", fake_run)
    governance_declared.run_build(
        discovery_module_root=module_root,
        declared=declared,
        config_path=cfg,
        target="spaces",
        force=True,
    )
    assert captured
    cmd = captured[0]
    assert "--spaces-only" in cmd
    assert "--force" in cmd
    assert str(declared) in cmd
