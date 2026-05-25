"""Tests for resolving CDF workflows to local transform pipelines."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from ui.server import transform_registry


def _write_pipeline(tmp_path: Path, pipeline_id: str, canvas: dict) -> None:
    instances = tmp_path / "transform" / "pipelines" / "instances"
    instances.mkdir(parents=True)
    reg_dir = tmp_path / "transform" / "pipelines"
    reg_dir.mkdir(parents=True, exist_ok=True)
    (reg_dir / "registry.yaml").write_text(
        f"schemaVersion: 1\npipelines:\n  - id: {pipeline_id}\n    label: Test\n",
        encoding="utf-8",
    )
    doc = {
        "schemaVersion": 1,
        "id": pipeline_id,
        "label": "Test",
        "canvas": canvas,
    }
    import yaml

    (instances / f"{pipeline_id}.yaml").write_text(yaml.safe_dump(doc), encoding="utf-8")


def test_find_pipeline_for_workflow_matches_canvas_start_node(tmp_path: Path) -> None:
    canvas = {
        "schemaVersion": 1,
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "position": {"x": 0, "y": 0},
                "data": {
                    "config": {
                        "workflow_external_id": "wf_all_etl_demo",
                    }
                },
            }
        ],
        "edges": [],
    }
    _write_pipeline(tmp_path, "demo", canvas)

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        found = transform_registry.find_pipeline_for_workflow("wf_all_etl_demo")

    assert found is not None
    assert found["pipeline_id"] == "demo"
    assert found["match"] == "canvas_start"


def test_find_pipeline_for_workflow_no_match(tmp_path: Path) -> None:
    reg_dir = tmp_path / "transform" / "pipelines"
    reg_dir.mkdir(parents=True)
    (reg_dir / "registry.yaml").write_text("schemaVersion: 1\npipelines: []\n", encoding="utf-8")

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        assert transform_registry.find_pipeline_for_workflow("missing_wf") is None
