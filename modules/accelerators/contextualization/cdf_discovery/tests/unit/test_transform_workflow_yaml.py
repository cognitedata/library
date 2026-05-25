from __future__ import annotations

from pathlib import Path

import pytest

from ui.server import transform_registry


def test_list_pipeline_workflow_artifacts(tmp_path: Path, monkeypatch) -> None:
    scope_dir = tmp_path / "transform" / "workflows" / "all"
    scope_dir.mkdir(parents=True)
    (scope_dir / "etl_demo.all.WorkflowTrigger.yaml").write_text("externalId: trg\n", encoding="utf-8")

    monkeypatch.setattr(transform_registry, "_module_root", lambda: tmp_path)
    artifacts = transform_registry.list_pipeline_workflow_artifacts("demo", scope_suffix="all")
    assert len(artifacts) == 1
    assert artifacts[0]["rel_path"] == "transform/workflows/all/etl_demo.all.WorkflowTrigger.yaml"


def test_read_write_workflow_yaml(tmp_path: Path, monkeypatch) -> None:
    rel = "transform/workflows/all/etl_demo.all.WorkflowTrigger.yaml"
    path = tmp_path / rel
    path.parent.mkdir(parents=True)
    path.write_text("externalId: trg\n", encoding="utf-8")

    monkeypatch.setattr(transform_registry, "_module_root", lambda: tmp_path)
    assert transform_registry.read_workflow_yaml(rel).startswith("externalId:")
    transform_registry.write_workflow_yaml(rel, "externalId: trg2\n")
    assert transform_registry.read_workflow_yaml(rel) == "externalId: trg2\n"

    with pytest.raises(ValueError):
        transform_registry.resolve_workflow_yaml_path("governance/foo.yaml")
