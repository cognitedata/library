"""Workflow task policy and codegen isAsyncComplete."""

from __future__ import annotations

import sys
from pathlib import Path

_TRANSFORM_ROOT = Path(__file__).resolve().parents[1]
_FUNCTIONS = _TRANSFORM_ROOT / "functions"
for p in (_TRANSFORM_ROOT, _FUNCTIONS):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import pytest

from cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: E402
    CanvasCompileError,
    compile_canvas_dag,
)
from cdf_fn_common.workflow_compile.codegen import build_workflow_version_document  # noqa: E402
from cdf_fn_common.workflow_task_policy import discovery_task_workflow_policy  # noqa: E402


def test_default_etl_function_policy_skip_task():
    pol = discovery_task_workflow_policy("fn_etl_view_query")
    assert pol["onFailure"] == "abortWorkflow"


def test_diagram_batch_policy_async():
    pol = discovery_task_workflow_policy("fn_etl_file_annotation")
    assert pol["isAsyncComplete"] is True
    assert pol["onFailure"] == "abortWorkflow"


def test_file_pattern_extract_compiles_dynamic_task():
    template = (
        Path(__file__).resolve().parents[2]
        / "workflow_definitions"
        / "templates"
        / "file_pattern_extract.template.yaml"
    )
    assert template.is_file(), template
    import yaml

    doc = yaml.safe_load(template.read_text(encoding="utf-8"))
    compiled = compile_canvas_dag(doc["canvas"])
    wv = build_workflow_version_document(
        workflow_external_id="wf_test_file_pattern",
        version="1",
        compiled_workflow=compiled,
        description="test",
    )
    tasks = wv["workflowDefinition"]["tasks"]
    types = {t["type"] for t in tasks}
    assert "dynamic" in types
    dynamic = next(t for t in tasks if t["type"] == "dynamic")
    assert "fanout_plan.output.tasks" in dynamic["parameters"]["dynamic"]["tasks"]
    map_dm = next(t for t in tasks if t["externalId"] == "map_annotations_dm")
    assert map_dm["type"] == "jsonMapping"
    assert map_dm["dependsOn"] == [{"externalId": "fanout"}]
    assert map_dm["parameters"]["jsonMapping"]["expression"] == "input.rows"
    assert map_dm["parameters"]["jsonMapping"]["input"]["rows"] == "${fanout.output}"


def test_json_mapping_compiles_to_cdf_task():
    canvas = {
        "nodes": [
            {
                "id": "map_rows",
                "kind": "json_mapping",
                "data": {
                    "config": {
                        "description": "Shape upstream output",
                        "input": {"rows": "${query_assets.output}"},
                        "expression": "input",
                    }
                },
            }
        ],
        "edges": [],
    }
    compiled = compile_canvas_dag(canvas)
    wv = build_workflow_version_document(
        workflow_external_id="wf_test_json_mapping",
        version="1",
        compiled_workflow=compiled,
        description="test",
    )
    jm = next(t for t in wv["workflowDefinition"]["tasks"] if t["type"] == "jsonMapping")
    assert jm["externalId"] == "map_rows"
    assert jm["name"] == "JSON mapping"
    assert jm["parameters"]["jsonMapping"]["expression"] == "input"
    assert jm["parameters"]["jsonMapping"]["input"]["rows"] == "${query_assets.output}"


def test_json_mapping_defaults_expression_at_compile():
    canvas = {
        "nodes": [
            {
                "id": "map_rows",
                "kind": "json_mapping",
                "data": {"config": {"mapper_kind": "custom", "input": {}, "expression": ""}},
            }
        ],
        "edges": [],
    }
    compiled = compile_canvas_dag(canvas)
    cfg = next(t for t in compiled["tasks"] if t["id"] == "map_rows")["payload"]["config"]
    assert cfg["expression"] == "input"


def test_diagram_json_mapping_wires_source_task_id():
    canvas = {
        "nodes": [
            {"id": "fanout", "kind": "dynamic_fanout", "data": {"config": {}}},
            {
                "id": "map_dm",
                "kind": "json_mapping",
                "data": {
                    "config": {
                        "mapper_kind": "diagram_detect_to_dm",
                        "input": {"rows": "${fanout.output}"},
                    }
                },
            },
        ],
        "edges": [
            {
                "source": "fanout",
                "target": "map_dm",
                "source_handle": "out",
                "target_handle": "in",
            }
        ],
    }
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "map_dm")
    assert task["task_type"] == "jsonMapping"
    assert task["payload"]["source_task_id"] == "fanout"
    assert "compile_as" not in task["payload"]["config"]


def test_legacy_field_map_raises_compile_error():
    canvas = {
        "nodes": [{"id": "old_map", "kind": "field_map", "data": {"config": {"mappings": []}}}],
        "edges": [],
    }
    with pytest.raises(CanvasCompileError, match="field_map"):
        compile_canvas_dag(canvas)
