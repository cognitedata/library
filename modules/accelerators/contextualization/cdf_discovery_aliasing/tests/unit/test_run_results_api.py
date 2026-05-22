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


def _v2_discovery_run(
    *,
    run_scope: dict | None = None,
    pipeline_tasks: list | None = None,
    persistence_nodes: list | None = None,
    merged_entities: dict | None = None,
    end_of_process: dict | None = None,
) -> dict:
    return {
        "schema_version": 2,
        "run_scope": run_scope or {"target": "workflow_local"},
        "run_id": "test-run",
        "dry_run": False,
        "end_of_process": end_of_process
        or {"status": "succeeded", "task_count": len(pipeline_tasks or []), "elapsed_ms": 100},
        "pipeline": {
            "task_count": len(pipeline_tasks or []),
            "tasks": pipeline_tasks or [],
        },
        "persistence": {
            "node_count": len(persistence_nodes or []),
            "nodes": persistence_nodes or [],
            "merged_entities": merged_entities or {},
        },
    }


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


def test_discovery_run_results_lists_v2(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_121212"
    rs = {"target": "workflow_local", "config_rel": "workflow.local.config.yaml"}
    (lr / f"{stem}_discovery_run.json").write_text(
        json.dumps(_v2_discovery_run(run_scope=rs, end_of_process={"status": "succeeded", "task_count": 0})),
        encoding="utf-8",
    )
    r = client.get("/api/run-results/discovery")
    assert r.status_code == 200
    runs = r.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["stem"] == stem
    assert runs[0]["run_rel"] == f"local_run_results/{stem}_discovery_run.json"


def test_discovery_run_results_filter_uses_run_scope(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_151515"
    rs = {"target": "workflow_template", "config_rel": "workflow.template.config.yaml"}
    (lr / f"{stem}_discovery_run.json").write_text(
        json.dumps(_v2_discovery_run(run_scope=rs)),
        encoding="utf-8",
    )
    r = client.get("/api/run-results/discovery", params={"run_scope_key": "workflow_template"})
    assert r.status_code == 200
    assert len(r.json()["runs"]) == 1
    r2 = client.get("/api/run-results/discovery", params={"run_scope_key": "workflow_local"})
    body2 = r2.json()
    assert body2["runs"] == []
    assert body2["total_all_scopes"] == 1
    assert body2["scope_filter"] == "workflow_local"


def test_discovery_run_detail_and_pipeline_tasks(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_141414"
    run_rel = f"local_run_results/{stem}_discovery_run.json"
    (lr / f"{stem}_discovery_run.json").write_text(
        json.dumps(
            _v2_discovery_run(
                pipeline_tasks=[
                    {
                        "task_id": "kea__a",
                        "category": "transform",
                        "status": "succeeded",
                        "output": {"function_external_id": "fn_dm_transform", "rows_read": 3},
                    },
                    {"task_id": "kea__b", "category": "other", "status": "failed", "error": "boom"},
                ],
                end_of_process={"status": "succeeded", "task_count": 2, "elapsed_ms": 1200},
            )
        ),
        encoding="utf-8",
    )
    r = client.get("/api/run-results/discovery-detail", params={"rel": run_rel})
    assert r.status_code == 200
    detail = r.json()
    assert detail["run_rel"] == run_rel
    assert detail["summary"]["status"] == "succeeded"
    r2 = client.get(
        "/api/run-results/discovery-pipeline-tasks",
        params={"rel": run_rel, "offset": 0, "limit": 1},
    )
    assert r2.status_code == 200
    tasks = r2.json()
    assert tasks["total"] == 2
    assert len(tasks["items"]) == 1
    assert tasks["items"][0]["output"]["rows_read"] == 3


def test_discovery_persistence_nodes(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_161616"
    run_rel = f"local_run_results/{stem}_discovery_run.json"
    (lr / f"{stem}_discovery_run.json").write_text(
        json.dumps(
            _v2_discovery_run(
                persistence_nodes=[
                    {
                        "task_id": "save_1",
                        "kind": "view_save",
                        "function_external_id": "fn_dm_view_save",
                        "input_cohort": {"entity_row_count": 2, "truncated": False},
                        "output": {"kind": "dm_instances_apply", "summary": {"instances_written": 2}},
                    },
                    {
                        "task_id": "ii_1",
                        "kind": "inverted_index",
                        "function_external_id": "fn_dm_inverted_index",
                        "input_cohort": {"entity_row_count": 5, "truncated": True},
                        "output": {"kind": "inverted_index_sink", "row_count": 3},
                    },
                ],
            )
        ),
        encoding="utf-8",
    )
    r = client.get("/api/run-results/discovery-persistence-nodes", params={"rel": run_rel})
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 2
    by_id = {x["task_id"]: x for x in body["items"]}
    assert by_id["save_1"]["input_cohort"]["entity_row_count"] == 2
    assert by_id["ii_1"]["output"]["row_count"] == 3

    r2 = client.get(
        "/api/run-results/discovery-persistence-node",
        params={"rel": run_rel, "task_id": "save_1"},
    )
    assert r2.status_code == 200
    assert r2.json()["node"]["task_id"] == "save_1"
    assert r2.json()["node"]["input_cohort"]["entity_row_count"] == 2


def test_discovery_persistence_merged(api_client) -> None:
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_161717"
    run_rel = f"local_run_results/{stem}_discovery_run.json"
    merged = {
        "instance_count": 1,
        "instances": [
            {
                "instance_key": "sp:a",
                "properties": {"aliases": ["x"], "indexKey": ["x"]},
            }
        ],
    }
    (lr / f"{stem}_discovery_run.json").write_text(
        json.dumps(_v2_discovery_run(merged_entities=merged)),
        encoding="utf-8",
    )
    r = client.get("/api/run-results/discovery-persistence-merged", params={"rel": run_rel})
    assert r.status_code == 200
    body = r.json()
    merged_out = body["merged_entities"]
    assert merged_out["instance_count"] == 1
    assert merged_out["instances"][0]["properties"]["aliases"] == ["x"]


def test_discovery_rejects_non_v2_path(api_client):
    client, root = api_client
    lr = root / "local_run_results"
    lr.mkdir(parents=True)
    stem = "20260205_999999"
    rel = f"local_run_results/{stem}_local_run_report.json"
    (lr / f"{stem}_local_run_report.json").write_text("{}", encoding="utf-8")
    r = client.get("/api/run-results/discovery-detail", params={"rel": rel})
    assert r.status_code == 400
