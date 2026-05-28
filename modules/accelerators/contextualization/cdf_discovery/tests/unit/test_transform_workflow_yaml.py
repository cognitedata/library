"""Built workflow YAML read/write under flat workflows/."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from ui.server import transform_registry


def test_list_and_read_workflow_yaml(tmp_path: Path) -> None:
    scope_dir = tmp_path / "workflows"
    scope_dir.mkdir(parents=True)
    rel_name = "etl_demo.WorkflowTrigger.yaml"
    (scope_dir / rel_name).write_text("externalId: trg\n", encoding="utf-8")

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        artifacts = transform_registry.list_pipeline_workflow_artifacts("demo", scope_suffix="")
    assert len(artifacts) == 1
    assert artifacts[0]["rel_path"] == "workflows/etl_demo.WorkflowTrigger.yaml"


def test_resolve_workflow_yaml_path(tmp_path: Path) -> None:
    rel = "workflows/etl_demo.WorkflowTrigger.yaml"
    (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / rel).write_text("x: 1\n", encoding="utf-8")

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        assert transform_registry.read_workflow_yaml(rel).startswith("x:")
