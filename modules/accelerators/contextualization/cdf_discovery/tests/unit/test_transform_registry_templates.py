"""Tests for transform registry template delete."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from ui.server import transform_registry


def test_delete_template_document_removes_file(tmp_path: Path) -> None:
    templates_dir = tmp_path / "transform" / "workflow_definitions" / "templates"
    templates_dir.mkdir(parents=True)
    template_path = templates_dir / "sample.template.yaml"
    template_path.write_text("template_id: sample\nlabel: Sample\n", encoding="utf-8")

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        transform_registry.delete_template_document("sample")

    assert not template_path.is_file()


def test_delete_template_document_missing_raises(tmp_path: Path) -> None:
    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        with pytest.raises(FileNotFoundError, match="Template not found"):
            transform_registry.delete_template_document("missing")


def test_update_pipeline_label_updates_registry(tmp_path: Path) -> None:
    instances_dir = tmp_path / "transform" / "workflow_definitions" / "instances"
    instances_dir.mkdir(parents=True)
    registry_path = tmp_path / "transform" / "workflow_definitions" / "registry.yaml"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        "schemaVersion: 1\nworkflows:\n  - id: sample\n    label: Old\n",
        encoding="utf-8",
    )
    instance_path = instances_dir / "sample.yaml"
    instance_path.write_text("id: sample\nlabel: Old\n", encoding="utf-8")

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        doc = transform_registry.update_pipeline_label("sample", "New label")
        entry = transform_registry.get_registry_entry("sample")

    assert doc["label"] == "New label"
    assert "label: New label" in instance_path.read_text(encoding="utf-8")
    assert entry is not None
    assert entry["label"] == "New label"


def test_update_template_label_writes_file(tmp_path: Path) -> None:
    templates_dir = tmp_path / "transform" / "workflow_definitions" / "templates"
    templates_dir.mkdir(parents=True)
    template_path = templates_dir / "sample.template.yaml"
    template_path.write_text("template_id: sample\nlabel: Old\n", encoding="utf-8")

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        doc = transform_registry.update_template_label("sample", "New label")

    assert doc["label"] == "New label"
    assert "label: New label" in template_path.read_text(encoding="utf-8")


def test_template_run_stream_argv_includes_template_flag(tmp_path: Path) -> None:
    templates_dir = tmp_path / "transform" / "workflow_definitions" / "templates"
    templates_dir.mkdir(parents=True)
    template_path = templates_dir / "sample.template.yaml"
    template_path.write_text(
        "template_id: sample\nlabel: Sample\ncanvas:\n  schemaVersion: 1\n  nodes: []\n  edges: []\n",
        encoding="utf-8",
    )

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        with patch.object(transform_registry, "_compile_canvas", return_value={"tasks": []}):
            argv = transform_registry.template_run_stream_argv(
                "sample", incremental_change_processing=False, dry_run=True
            )

    assert "--template" in argv
    assert argv[argv.index("--template") + 1] == "sample"
    assert "--no-incremental-change-processing" in argv
    assert "--dry-run" in argv


def test_build_template_uses_in_process_workflow_build(tmp_path: Path) -> None:
    templates_dir = tmp_path / "transform" / "workflow_definitions" / "templates"
    templates_dir.mkdir(parents=True)
    template_path = templates_dir / "sample.template.yaml"
    template_path.write_text(
        "template_id: sample\nlabel: Sample\ncanvas:\n  schemaVersion: 1\n  nodes: []\n  edges: []\n",
        encoding="utf-8",
    )
    config_path = tmp_path / "transform" / "default.config.yaml"
    config_path.write_text("workflow: wf_all_etl_global\n", encoding="utf-8")

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        with patch.object(transform_registry, "_compile_canvas", return_value={"tasks": []}):
            with patch("workflow_build.orchestrate.run_build") as run_mock:
                run_mock.return_value = {"ok": True, "written": [], "errors": []}
                result = transform_registry.build_template("sample")

    assert result["ok"] is True
    run_mock.assert_called_once()
    assert run_mock.call_args.kwargs.get("template_ids") == ["sample"]


def test_template_build_pairing_uses_per_template_workflow_base(tmp_path: Path) -> None:
    templates_dir = tmp_path / "transform" / "workflow_definitions" / "templates"
    templates_dir.mkdir(parents=True)
    (templates_dir / "sample.template.yaml").write_text(
        yaml.safe_dump(
            {
                "template_id": "sample",
                "label": "Sample",
                "canvas": {
                    "nodes": [
                        {
                            "id": "start",
                            "kind": "start",
                            "data": {"config": {"workflow_version": "1"}},
                        },
                        {"id": "end", "kind": "end"},
                    ],
                    "edges": [{"source": "start", "target": "end"}],
                },
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "transform" / "default.config.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("workflow: wf_all_etl_global\nworkflow_schedule: '0 2 * * *'\n", encoding="utf-8")

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        pairing = transform_registry.template_build_pairing("sample", scoped=False)

    assert pairing["workflow_base"] == "wf_all_etl_sample"
    assert pairing["workflow_external_id"] == "wf_all_etl_sample"
