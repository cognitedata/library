"""Tests for operator UI config API."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient


@pytest.fixture()
def client_with_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cfg_path = tmp_path / "default.config.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "name": "test",
                "organization": "TestOrg",
                "index_storage_backend": "raw",
                "index_raw_database": "db_contextualization_idx",
                "scope": {"enabled": False, "levels": [], "fallback_scope_key": "global"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CDF_INVERTED_INDEX_ROOT", str(tmp_path))
    import importlib

    import ui.server.paths as paths_mod
    import ui.server.main as main_mod

    importlib.reload(paths_mod)
    paths_mod.CONFIG_PATH = cfg_path
    paths_mod.WORKSPACE_PATH = tmp_path / ".ui_workspace.json"
    importlib.reload(main_mod)
    return TestClient(main_mod.app)


def test_get_config_returns_yaml_and_runtime(client_with_config: TestClient) -> None:
    res = client_with_config.get("/api/inverted-index/config")
    assert res.status_code == 200
    body = res.json()
    assert "yaml_text" in body
    assert body["runtime"]["storage_backend"] == "raw"
    assert body["runtime"]["scope_enabled"] is False


def test_put_config_rejects_invalid_yaml(client_with_config: TestClient) -> None:
    res = client_with_config.put(
        "/api/inverted-index/config",
        json={"yaml_text": "scope: [not, a, mapping"},
    )
    assert res.status_code == 400


def test_put_config_validates_scope(client_with_config: TestClient) -> None:
    bad = yaml.safe_dump(
        {
            "name": "test",
            "scope": {
                "enabled": True,
                "levels": ["site"],
                "resolve_from": {},
            },
        }
    )
    res = client_with_config.put("/api/inverted-index/config", json={"yaml_text": bad})
    assert res.status_code == 400


def test_put_config_persists_valid_update(client_with_config: TestClient) -> None:
    updated = yaml.safe_dump(
        {
            "name": "test",
            "organization": "TestOrg",
            "index_storage_backend": "raw",
            "index_raw_database": "db_test_idx",
            "scope": {"enabled": False, "levels": [], "fallback_scope_key": "global"},
        }
    )
    res = client_with_config.put("/api/inverted-index/config", json={"yaml_text": updated})
    assert res.status_code == 200
    assert res.json()["runtime"]["raw_database"] == "db_test_idx"
