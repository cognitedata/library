"""Workflow graph API parses WorkflowVersion tasks."""

from pathlib import Path

import yaml


def test_workflow_version_has_three_tasks():
    root = Path(__file__).resolve().parents[2]
    path = root / "workflows" / "create_asset_hierarchy_from_files.WorkflowVersion.yaml"
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    tasks = raw["workflowDefinition"]["tasks"]
    assert len(tasks) == 3
    ids = [t["externalId"] for t in tasks]
    assert ids[0] == "fn_dm_extract_assets_by_pattern"
    assert ids[1] == "fn_dm_create_asset_hierarchy"
    assert ids[2] == "fn_dm_write_asset_hierarchy"
    assert tasks[1]["dependsOn"][0]["externalId"] == "fn_dm_extract_assets_by_pattern"
