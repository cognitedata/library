"""Tests for operator API run-results endpoints."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_MODULE = Path(__file__).resolve().parents[2]
if str(_MODULE) not in sys.path:
    sys.path.insert(0, str(_MODULE))


@pytest.fixture()
def api_client(monkeypatch, tmp_path: Path):
    tmp_path = tmp_path.resolve()
    import ui.server.main as main

    monkeypatch.setattr(main, "MODULE_ROOT", tmp_path)
    return TestClient(main.app), tmp_path


def test_run_results_empty(api_client):
    client, _root = api_client
    r = client.get("/api/run-results")
    assert r.status_code == 200
    assert r.json() == {"runs": []}


def test_run_results_lists_pair(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260203_010203"
    (lr / f"{stem}_cdf_extraction.json").write_text(
        json.dumps({"results": [{"k": 1}]}), encoding="utf-8"
    )
    (lr / f"{stem}_cdf_aliasing.json").write_text(json.dumps({"results": []}), encoding="utf-8")
    r = client.get("/api/run-results")
    assert r.status_code == 200
    runs = r.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["stem"] == stem
    assert runs[0]["extraction_rel"] == f"local_run_results/{stem}_cdf_extraction.json"


def test_run_results_preview(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260203_010203"
    rel = f"local_run_results/{stem}_cdf_extraction.json"
    payload = {"results": [{"n": i} for i in range(5)]}
    (lr / f"{stem}_cdf_extraction.json").write_text(json.dumps(payload), encoding="utf-8")
    (lr / f"{stem}_cdf_aliasing.json").write_text(json.dumps({"results": []}), encoding="utf-8")
    r = client.get("/api/run-results/preview", params={"rel": rel, "offset": 1, "limit": 2})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 5
    assert data["offset"] == 1
    assert len(data["items"]) == 2
    assert data["items"][0]["n"] == 1


def test_run_results_preview_rejects_non_pipeline_path(api_client):
    client, _root = api_client
    r = client.get(
        "/api/run-results/preview",
        params={"rel": "workflow.local.config.yaml", "offset": 0, "limit": 10},
    )
    assert r.status_code == 400


def test_run_results_filter_excludes_legacy_without_run_scope(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    for stem, rs in (
        ("20260204_010101", {"target": "workflow_local", "config_rel": "workflow.local.config.yaml"}),
        ("20260204_020202", None),
    ):
        ex = lr / f"{stem}_cdf_extraction.json"
        al = lr / f"{stem}_cdf_aliasing.json"
        body: dict = {"results": []}
        if rs is not None:
            body["run_scope"] = rs
        ex.write_text(json.dumps(body), encoding="utf-8")
        al.write_text(json.dumps({"results": []}), encoding="utf-8")
    r = client.get("/api/run-results", params={"run_scope_key": "workflow_local"})
    assert r.status_code == 200
    stems = {x["stem"] for x in r.json()["runs"]}
    assert stems == {"20260204_010101"}


def test_run_results_filter_workflow_trigger(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    trig = "workflows/site_a/foo.WorkflowTrigger.yaml"
    stem = "20260204_030303"
    rs = {"target": "workflow_trigger", "workflow_trigger_rel": trig, "config_rel": ".operator_run_scope.yaml"}
    (lr / f"{stem}_cdf_extraction.json").write_text(
        json.dumps({"run_scope": rs, "results": []}), encoding="utf-8"
    )
    (lr / f"{stem}_cdf_aliasing.json").write_text(json.dumps({"run_scope": rs, "results": []}), encoding="utf-8")
    r = client.get("/api/run-results", params={"run_scope_key": f"workflow_trigger:{trig}"})
    assert r.status_code == 200
    assert len(r.json()["runs"]) == 1
    r2 = client.get(
        "/api/run-results",
        params={"run_scope_key": "workflow_trigger:workflows/other/bar.WorkflowTrigger.yaml"},
    )
    assert r2.json()["runs"] == []
