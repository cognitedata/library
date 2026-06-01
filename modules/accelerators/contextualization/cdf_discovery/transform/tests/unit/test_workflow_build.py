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

from workflow_build.build_scoped import build_scoped_workflow  # noqa: E402
from workflow_build.ids import resolve_workflow_base_for_build  # noqa: E402
from workflow_build.orchestrate import run_build  # noqa: E402
from workflow_build.sources import load_template, template_document_for_build  # noqa: E402
from workflow_build.targets import ScopedWorkflowTarget  # noqa: E402
from workflow_build.workflow_document_limits import assert_workflow_document_within_limit  # noqa: E402
from workflow_build.workflow_document_trim import (  # noqa: E402
    build_trigger_input,
    trim_workflow_document_for_deploy,
)


def test_build_workflow_instance_dry_run(tmp_path: Path) -> None:
    inst_dir = tmp_path / "workflow_definitions" / "instances"
    inst_dir.mkdir(parents=True)
    inst_path = inst_dir / "test_inst.yaml"
    inst_path.write_text(
        yaml.safe_dump(
            {
                "id": "test_inst",
                "label": "Test",
                "canvas": {
                    "nodes": [
                        {
                            "id": "start",
                            "kind": "start",
                            "data": {
                                "config": {
                                    "trigger_type": "schedule",
                                    "cron_expression": "30 4 * * *",
                                    "workflow_version": "2",
                                }
                            },
                        },
                        {"id": "q1", "kind": "query_view", "data": {"config": {"view_external_id": "Asset"}}},
                        {"id": "end", "kind": "end"},
                    ],
                    "edges": [
                        {"source": "start", "target": "q1"},
                        {"source": "q1", "target": "end"},
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    config = {
        "workflow": "wf_all_etl_test",
        "dataset": "ds_discovery_etl",
        "workflow_definitions": {
            "instances_dir": "workflow_definitions/instances",
            "templates_dir": "workflow_definitions/templates",
        },
    }
    result = run_build(
        module_root=tmp_path,
        config=config,
        workflow_ids=["test_inst"],
        dry_run=True,
    )
    assert result["ok"]
    assert any("workflows/etl_test_inst.config.yaml" in str(p) for p in result["written"])
    assert not (tmp_path / "workflows" / "all").exists()

    from workflow_build.sources import load_instance

    source = load_instance(tmp_path, "test_inst", config)
    target = ScopedWorkflowTarget(
        workflow_id="test_inst",
        scope_suffix="",
        scope_id="",
        node_chain=[],
        segment_ids=[],
        source_kind="instance",
    )
    build_scoped_workflow(
        module_root=tmp_path,
        config=config,
        source=source,
        target=target,
        levels=[],
        dry_run=False,
    )
    trig = yaml.safe_load(
        (tmp_path / "workflows" / "etl_test_inst.WorkflowTrigger.yaml").read_text(encoding="utf-8")
    )
    assert trig["triggerRule"]["cronExpression"] == "30 4 * * *"
    assert trig["workflowVersion"] == "2"
    assert trig["workflowExternalId"] == "wf_etl_test_inst"
    assert trig["externalId"] == "trg_wf_etl_test_inst"


def _minimal_canvas() -> dict:
    return {
        "nodes": [
            {
                "id": "start",
                "kind": "start",
                "data": {
                    "config": {
                        "trigger_type": "schedule",
                        "cron_expression": "30 4 * * *",
                        "workflow_version": "2",
                    }
                },
            },
            {"id": "q1", "kind": "query_view", "data": {"config": {"view_external_id": "Asset"}}},
            {"id": "end", "kind": "end"},
        ],
        "edges": [
            {"source": "start", "target": "q1"},
            {"source": "q1", "target": "end"},
        ],
    }


def test_build_template_workflow_base(tmp_path: Path) -> None:
    tpl_dir = tmp_path / "workflow_definitions" / "templates"
    tpl_dir.mkdir(parents=True)
    tpl_path = tpl_dir / "my_tpl.template.yaml"
    tpl_path.write_text(
        yaml.safe_dump(
            {"template_id": "my_tpl", "label": "My template", "canvas": _minimal_canvas()}
        ),
        encoding="utf-8",
    )
    config = {"workflow": "wf_all_etl_global", "dataset": "ds_discovery_etl"}
    result = run_build(module_root=tmp_path, config=config, template_ids=["my_tpl"], dry_run=True)
    assert result["ok"]
    assert any("workflows/etl_my_tpl.config.yaml" in str(p) for p in result["written"])

    source = load_template(tmp_path, "my_tpl", config)
    target = ScopedWorkflowTarget(
        workflow_id="my_tpl",
        scope_suffix="",
        scope_id="",
        node_chain=[],
        segment_ids=[],
        source_kind="template",
    )
    build_scoped_workflow(
        module_root=tmp_path,
        config=config,
        source=source,
        target=target,
        levels=[],
        dry_run=False,
    )
    assert not (tmp_path / "workflow_definitions" / "instances" / "my_tpl.yaml").is_file()
    trig = yaml.safe_load(
        (tmp_path / "workflows" / "etl_my_tpl.WorkflowTrigger.yaml").read_text(encoding="utf-8")
    )
    assert trig["workflowExternalId"] == "wf_etl_my_tpl"


def test_resolve_workflow_base_for_build_template_defaults_per_id() -> None:
    config = {}
    assert (
        resolve_workflow_base_for_build(
            source_kind="template",
            config=config,
            workflow_id="aliasing_template",
            canvas={},
        )
        == "wf_etl_aliasing_template"
    )
    assert (
        resolve_workflow_base_for_build(
            source_kind="instance",
            config=config,
            workflow_id="aliasing_template",
            canvas={},
        )
        == "wf_etl_aliasing_template"
    )


def test_trim_and_limit_small_doc() -> None:
    doc = {"canvas": {"nodes": [{"id": "n1", "kind": "start", "position": {"x": 1}}]}}
    trimmed = trim_workflow_document_for_deploy(doc)
    assert "position" not in trimmed["canvas"]["nodes"][0]
    assert_workflow_document_within_limit(trimmed, workflow_id="x")


def test_build_trigger_input_prefers_start_node_incremental_flag() -> None:
    doc = {
        "schemaVersion": 1,
        "id": "wf_test",
        "parameters": {"incremental_change_processing": False},
        "canvas": {
            "nodes": [
                {
                    "id": "start",
                    "kind": "start",
                    "data": {
                        "config": {
                            "trigger_type": "schedule",
                            "cron_expression": "0 2 * * *",
                            "incremental_change_processing": True,
                        }
                    },
                }
            ]
        },
    }
    trigger_input = build_trigger_input(doc)
    assert trigger_input["incremental_change_processing"] is True
    assert trigger_input["configuration"]["parameters"]["incremental_change_processing"] is True


def test_build_trigger_input_defaults_true_without_start_node_value() -> None:
    doc = {
        "schemaVersion": 1,
        "id": "wf_test",
        "parameters": {"incremental_change_processing": "false"},
        "canvas": {"nodes": []},
    }
    trigger_input = build_trigger_input(doc)
    assert trigger_input["incremental_change_processing"] is True
    assert trigger_input["configuration"]["parameters"]["incremental_change_processing"] is True
