"""Unit tests for file annotation building blocks and fan-out compile."""

from __future__ import annotations

import sys
from pathlib import Path

_TRANSFORM_ROOT = Path(__file__).resolve().parents[1]
_FUNCTIONS = _TRANSFORM_ROOT / "functions"
for p in (_TRANSFORM_ROOT, _FUNCTIONS):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from cdf_fn_common.etl_annotation_map.expand import (  # noqa: E402
    expand_cohort_rows_to_classic_rows,
    expand_cohort_rows_to_dm_rows,
)
from cdf_fn_common.etl_file_annotation.entities import (  # noqa: E402
    entities_from_cohort_rows,
    is_prebuilt_detect_entities,
    resolve_file_annotation_entities,
)
from cdf_fn_common.etl_file_annotation.files import files_from_cohort_rows  # noqa: E402
from cdf_fn_common.etl_fanout_plan.registry import get_fanout_profile  # noqa: E402
from cdf_fn_common.workflow_compile.canvas_dag import (  # noqa: E402
    FANOUT_PLAN_HANDLE_INPUT_A,
    FANOUT_PLAN_HANDLE_INPUT_B,
    FILE_ANNOTATION_HANDLE_ENTITIES,
    FILE_ANNOTATION_HANDLE_FILES,
    compile_canvas_dag,
)


def test_is_prebuilt_detect_entities():
    assert is_prebuilt_detect_entities([{"sample": ["00-X-00"]}])
    assert not is_prebuilt_detect_entities([{"columns": {}, "properties": {}}])


def test_entities_from_asset_cohort():
    rows = [
        {
            "columns": {"external_id": "a1"},
            "properties": {"aliases": ["FT-101A", "FT-101B"]},
        }
    ]
    entities = entities_from_cohort_rows(
        rows,
        {"patterns_entity_property": "aliases", "pattern_resource_type": "equipment"},
    )
    assert entities
    assert entities[0].get("sample")


def test_files_from_cohort_rows():
    rows = [
        {
            "columns": {"external_id": "f1"},
            "properties": {"id": 42, "page_count": 3, "name": "doc.pdf"},
        }
    ]
    files = files_from_cohort_rows(rows)
    assert len(files) == 1
    assert files[0]["id"] == 42


def test_resolve_entities_prebuilt_payload():
    data = {"entities": [{"sample": ["TAG-1"], "category": "equipment"}]}
    out = resolve_file_annotation_entities(data, {}, client=None)
    assert out[0]["sample"] == ["TAG-1"]


def test_fanout_profile_registry():
    profile = get_fanout_profile("file_annotation")
    assert profile.name == "file_annotation"
    assert profile.required_handles({})["input_a"] is True


def test_compile_file_annotation_dual_handles():
    canvas = {
        "nodes": [
            {"id": "ctx", "kind": "query_view", "data": {"config": {}}},
            {"id": "files", "kind": "query_view", "data": {"config": {}}},
            {
                "id": "annotate",
                "kind": "file_annotation",
                "data": {"config": {"description": "annotate"}},
            },
        ],
        "edges": [
            {
                "source": "ctx",
                "target": "annotate",
                "source_handle": "out",
                "target_handle": FILE_ANNOTATION_HANDLE_ENTITIES,
            },
            {
                "source": "files",
                "target": "annotate",
                "source_handle": "out",
                "target_handle": FILE_ANNOTATION_HANDLE_FILES,
            },
        ],
    }
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "annotate")
    assert task["function_external_id"] == "fn_etl_file_annotation"
    assert task["payload"]["entities_input_task_id"] == "ctx"
    assert task["payload"]["files_input_task_id"] == "files"


def test_compile_fanout_plan_dual_handles():
    canvas = {
        "nodes": [
            {"id": "ctx", "kind": "query_view", "data": {"config": {}}},
            {"id": "files", "kind": "query_view", "data": {"config": {}}},
            {
                "id": "plan",
                "kind": "workflow_fanout_plan",
                "data": {"config": {"fanout_profile": "file_annotation"}},
            },
        ],
        "edges": [
            {
                "source": "ctx",
                "target": "plan",
                "source_handle": "out",
                "target_handle": FANOUT_PLAN_HANDLE_INPUT_A,
            },
            {
                "source": "files",
                "target": "plan",
                "source_handle": "out",
                "target_handle": FANOUT_PLAN_HANDLE_INPUT_B,
            },
        ],
    }
    compiled = compile_canvas_dag(canvas)
    task = next(t for t in compiled["tasks"] if t["id"] == "plan")
    assert task["payload"]["input_a_task_id"] == "ctx"
    assert task["payload"]["input_b_task_id"] == "files"


def test_compile_json_mapping_diagram_detect_to_cdf_json_mapping():
    canvas = {
        "nodes": [
            {"id": "fanout", "kind": "dynamic_fanout", "data": {"config": {}}},
            {
                "id": "map_dm",
                "kind": "json_mapping",
                "data": {
                    "config": {
                        "mapper_kind": "diagram_detect_to_dm",
                        "annotation_space": "discovery-annotations",
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
    assert task["executable_kind"] == "json_mapping"
    assert task["payload"]["config"]["expression"] == "input.rows"
    assert task["payload"]["config"]["mapper_kind"] == "diagram_detect_to_dm"


def test_expand_dm_rows_from_cohort_hit():
    rows = [
        {
            "columns": {"external_id": "f1", "node_instance_id": "s:f1"},
            "properties": {
                "text": "FT-101",
                "region": {"page": 1, "x": 0.1, "y": 0.2, "width": 0.1, "height": 0.1},
                "file_ref": {"file_id": 1, "page_number": 1},
                "annotation": {"text": "FT-101", "entities": []},
            },
        }
    ]
    dm = expand_cohort_rows_to_dm_rows(rows, {"annotation_space": "ann-space"})
    assert dm
    assert dm[0]["annotation_space"] == "ann-space"
    classic = expand_cohort_rows_to_classic_rows(rows, {})
    assert classic[0]["text"] == "FT-101"
