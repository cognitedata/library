from __future__ import annotations

import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPTS = ROOT / "scripts"
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(SCRIPTS), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from workflow_build.check import validate_artifact_set  # noqa: E402


def _write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def test_validate_artifact_set_passes_for_matching_triplet(tmp_path: Path) -> None:
    wf = tmp_path / "workflows" / "etl_x.Workflow.yaml"
    wv = tmp_path / "workflows" / "etl_x.WorkflowVersion.yaml"
    trg = tmp_path / "workflows" / "etl_x.WorkflowTrigger.yaml"
    _write_yaml(
        wf,
        {
            "externalId": "wf_etl_x",
            "description": "x",
            "dataSetExternalId": "ds_discovery_etl",
        },
    )
    _write_yaml(
        wv,
        {
            "workflowExternalId": "wf_etl_x",
            "version": "1",
            "workflowDefinition": {
                "tasks": [
                    {
                        "externalId": "task_a",
                        "type": "function",
                        "parameters": {
                            "function": {
                                "externalId": "fn_etl_raw_cleanup",
                                "data": {
                                    "task_id": "task_a",
                                    "canvas_node_id": "task_a",
                                    "config": {"description": "ok"},
                                },
                            }
                        },
                    }
                ]
            },
        },
    )
    _write_yaml(
        trg,
        {
            "externalId": "trg_wf_etl_x",
            "workflowExternalId": "wf_etl_x",
            "workflowVersion": "1",
            "triggerRule": {"triggerType": "schedule", "cronExpression": "0 2 * * *"},
            "input": {
                "incremental_change_processing": True,
                "run_id": "",
                "configuration": {"schemaVersion": 1, "id": "x", "scope": None, "parameters": {}},
            },
        },
    )
    errors = validate_artifact_set(
        workflow_path=wf,
        workflow_version_path=wv,
        trigger_path=trg,
    )
    assert errors == []


def test_validate_artifact_set_catches_cross_file_mismatch(tmp_path: Path) -> None:
    wf = tmp_path / "workflows" / "etl_y.Workflow.yaml"
    wv = tmp_path / "workflows" / "etl_y.WorkflowVersion.yaml"
    trg = tmp_path / "workflows" / "etl_y.WorkflowTrigger.yaml"
    _write_yaml(wf, {"externalId": "wf_etl_y"})
    _write_yaml(
        wv,
        {
            "workflowExternalId": "wf_etl_other",
            "version": "2",
            "workflowDefinition": {"tasks": [{"externalId": "a", "type": "function"}]},
        },
    )
    _write_yaml(
        trg,
        {
            "externalId": "trg_wf_etl_other",
            "workflowExternalId": "wf_etl_other",
            "workflowVersion": "3",
            "triggerRule": {"triggerType": "schedule", "cronExpression": "0 2 * * *"},
            "input": {"run_id": ""},
        },
    )
    errors = validate_artifact_set(
        workflow_path=wf,
        workflow_version_path=wv,
        trigger_path=trg,
    )
    assert any("Workflow/WorkflowVersion mismatch" in e for e in errors)
    assert any("Workflow/WorkflowTrigger mismatch" in e for e in errors)
    assert any("WorkflowVersion/WorkflowTrigger mismatch" in e for e in errors)
    assert any("WorkflowTrigger.input invalid" in e for e in errors)
