"""Transform tree lists pipelines from build output under transform/workflows/."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from ui.server import discovery_tree, transform_registry


def test_list_built_scope_suffixes_and_entries(tmp_path: Path) -> None:
    workflows = tmp_path / "transform" / "workflows"
    (workflows / "all").mkdir(parents=True)
    (workflows / "global").mkdir(parents=True)
    (workflows / "all" / "etl_demo.all.config.yaml").write_text(
        yaml.safe_dump({"id": "demo", "label": "Demo"}),
        encoding="utf-8",
    )
    (workflows / "global" / "etl_discovery_etl_default.global.config.yaml").write_text(
        yaml.safe_dump({"id": "discovery_etl_default", "description": "Default"}),
        encoding="utf-8",
    )

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        scopes = transform_registry.list_built_scope_suffixes()
        assert scopes == ["all", "global"]
        all_rows = transform_registry.list_built_pipeline_entries(scope_suffix="all")
        assert len(all_rows) == 1
        assert all_rows[0]["id"] == "demo"
        global_rows = transform_registry.list_built_pipeline_entries(scope_suffix="global")
        assert global_rows[0]["id"] == "discovery_etl_default"


def test_list_instance_pipeline_entries(tmp_path: Path) -> None:
    instances = tmp_path / "transform" / "workflow_definitions" / "instances"
    instances.mkdir(parents=True)
    registry = tmp_path / "transform" / "workflow_definitions" / "registry.yaml"
    registry.parent.mkdir(parents=True, exist_ok=True)
    (instances / "data_wranglers.yaml").write_text(
        yaml.safe_dump({"id": "data_wranglers", "label": "Data wranglers"}),
        encoding="utf-8",
    )
    registry.write_text(
        yaml.safe_dump(
            {
                "schemaVersion": 1,
                "workflows": [{"id": "data_wranglers", "label": "Data wranglers"}],
            }
        ),
        encoding="utf-8",
    )

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        rows = transform_registry.list_instance_pipeline_entries()
    assert len(rows) == 1
    assert rows[0]["id"] == "data_wranglers"
    assert rows[0]["source"] == "instance"


def test_transform_tree_lists_pipelines_folder(tmp_path: Path) -> None:
    instances = tmp_path / "transform" / "workflow_definitions" / "instances"
    instances.mkdir(parents=True)
    (instances / "data_wranglers.yaml").write_text(
        yaml.safe_dump({"id": "data_wranglers", "label": "Data wranglers"}),
        encoding="utf-8",
    )
    (tmp_path / "transform" / "workflow_definitions" / "registry.yaml").write_text(
        yaml.safe_dump(
            {
                "schemaVersion": 1,
                "workflows": [{"id": "data_wranglers", "label": "Data wranglers"}],
            }
        ),
        encoding="utf-8",
    )

    client = MagicMock()
    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        root_children = discovery_tree.list_children(client, "transform")
    pipelines_folder = next(n for n in root_children if n["id"] == "transform:pipelines")
    assert pipelines_folder["has_children"] is True

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        pipeline_nodes = discovery_tree.list_children(client, "transform:pipelines")
    assert len(pipeline_nodes) == 1
    assert pipeline_nodes[0]["meta"]["id"] == "data_wranglers"


def test_transform_tree_lists_scope_folders_and_pipelines(tmp_path: Path) -> None:
    workflows = tmp_path / "transform" / "workflows" / "all"
    workflows.mkdir(parents=True)
    (workflows / "etl_demo.all.config.yaml").write_text(
        yaml.safe_dump({"id": "demo", "label": "Demo"}),
        encoding="utf-8",
    )

    client = MagicMock()
    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        with patch("ui.server.discovery_tree.cdf_browse.connection_info", return_value={"project": "test"}):
            root_children = discovery_tree.list_children(client, "transform")
    scope_ids = {n["id"] for n in root_children if n.get("kind") == "folder"}
    assert "transform:all" in scope_ids

    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        pipeline_nodes = discovery_tree.list_children(client, "transform:all")
    assert len(pipeline_nodes) == 1
    assert pipeline_nodes[0]["kind"] == "etl_pipeline"
    assert pipeline_nodes[0]["meta"]["id"] == "demo"
    assert pipeline_nodes[0]["meta"]["scope_suffix"] == "all"
    (workflows / "etl_demo.all.Workflow.yaml").write_text("externalId: wf\n", encoding="utf-8")
    with patch.object(transform_registry, "_module_root", return_value=tmp_path):
        wf_children = discovery_tree.list_children(client, pipeline_nodes[0]["id"])
    assert len(wf_children) >= 1
    assert wf_children[0]["kind"] == "etl_workflow_yaml"
