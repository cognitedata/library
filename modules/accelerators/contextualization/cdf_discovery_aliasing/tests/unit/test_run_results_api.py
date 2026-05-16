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


def test_discovery_run_results_lists_report(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_121212"
    rs = {"target": "workflow_local", "config_rel": "workflow.local.config.yaml"}
    (lr / f"{stem}_local_run_report.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "paths": {"discovery": f"local_run_results/{stem}_cdf_discovery_tasks.json"},
                "end_of_process": {"status": "succeeded", "task_count": 0},
            }
        ),
        encoding="utf-8",
    )
    (lr / f"{stem}_cdf_discovery_tasks.json").write_text(
        json.dumps({"run_scope": rs, "task_outputs": {}}),
        encoding="utf-8",
    )
    r = client.get("/api/run-results/discovery")
    assert r.status_code == 200
    runs = r.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["stem"] == stem
    assert runs[0]["report_rel"] == f"local_run_results/{stem}_local_run_report.json"


def test_discovery_run_results_filter_uses_run_scope_from_sidecar(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_151515"
    rs = {"target": "workflow_template", "config_rel": "workflow.template.config.yaml"}
    (lr / f"{stem}_local_run_report.json").write_text(
        json.dumps({"schema_version": 1, "paths": {}, "end_of_process": {}}),
        encoding="utf-8",
    )
    (lr / f"{stem}_cdf_discovery_tasks.json").write_text(json.dumps({"run_scope": rs}), encoding="utf-8")
    r = client.get("/api/run-results/discovery", params={"run_scope_key": "workflow_template"})
    assert r.status_code == 200
    assert len(r.json()["runs"]) == 1
    r2 = client.get("/api/run-results/discovery", params={"run_scope_key": "workflow_local"})
    assert r2.json()["runs"] == []


def test_discovery_raw_preview_paginates_rows(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_121212"
    rel = f"local_run_results/{stem}_local_run_report.json"
    rows = [{"key": f"k{i}", "columns": {"n": i}} for i in range(5)]
    raw_results = {
        "schema_version": 1,
        "tables": [
            {
                "raw_db": "db1",
                "raw_table": "t1",
                "source_task_ids": ["a"],
                "rows": rows,
            }
        ],
    }
    (lr / f"{stem}_local_run_report.json").write_text(
        json.dumps({"run_scope": {"target": "workflow_local"}, "raw_results": raw_results}),
        encoding="utf-8",
    )
    r = client.get(
        "/api/run-results/discovery-raw-preview",
        params={"rel": rel, "table_index": 0, "offset": 0, "limit": 2},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["has_raw_results"] is True
    assert data["tables_empty"] is False
    assert data["total"] == 5
    assert len(data["items"]) == 2
    assert data["items"][0]["key"] == "k0"
    assert "all_tables" in data
    assert len(data["all_tables"]) == 1
    assert data["all_tables"][0]["raw_db"] == "db1"
    r2 = client.get(
        "/api/run-results/discovery-raw-preview",
        params={"rel": rel, "table_index": 0, "offset": 1, "limit": 2},
    )
    assert r2.status_code == 200
    d2 = r2.json()
    assert d2["items"][0]["key"] == "k1"
    assert "all_tables" not in d2


def test_discovery_raw_preview_offset_nonzero_omits_all_tables(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_131313"
    rel = f"local_run_results/{stem}_local_run_report.json"
    rows = [{"key": f"k{i}", "columns": {}} for i in range(3)]
    (lr / f"{stem}_local_run_report.json").write_text(
        json.dumps(
            {
                "raw_results": {
                    "tables": [{"raw_db": "d", "raw_table": "t", "rows": rows}],
                }
            }
        ),
        encoding="utf-8",
    )
    r = client.get(
        "/api/run-results/discovery-raw-preview",
        params={"rel": rel, "offset": 2, "limit": 2},
    )
    assert r.status_code == 200
    assert "all_tables" not in r.json()


def test_discovery_run_results_rejects_bad_rel(api_client):
    client, _root = api_client
    r = client.get(
        "/api/run-results/discovery-raw-preview",
        params={"rel": "workflow.local.config.yaml", "offset": 0, "limit": 10},
    )
    assert r.status_code == 400
