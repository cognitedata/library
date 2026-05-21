"""Tests for discovery run results schema v2."""

from __future__ import annotations

import sys
from pathlib import Path

_MODULE = Path(__file__).resolve().parents[2]
_FUNCS = _MODULE / "functions"
for _p in (_FUNCS, _MODULE):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from local_runner.discovery_run_v2 import (  # noqa: E402
    DISCOVERY_RUN_SCHEMA_VERSION,
    build_end_of_process,
    compose_discovery_run_document,
)


def test_build_end_of_process_marks_failed_task() -> None:
    eop = build_end_of_process(
        tasks=[{"task_id": "t1", "status": "failed"}],
        wall_t0=0.0,
        dry_run=False,
    )
    assert eop["status"] == "failed"
    assert eop["failed_task_key"] == "t1"


def test_compose_discovery_run_v2_persistence_nodes() -> None:
    doc = compose_discovery_run_document(
        run_scope={"target": "workflow_local"},
        run_id="run-1",
        dry_run=False,
        wall_t0=0.0,
        local_run_tasks=[
            {
                "task_id": "save_1",
                "function_external_id": "fn_dm_view_save",
                "status": "succeeded",
                "duration_sec": 1.5,
                "output": {"instances_written": 2},
            },
            {
                "task_id": "q1",
                "function_external_id": "fn_dm_transform",
                "status": "succeeded",
                "duration_sec": 0.2,
            },
        ],
        discovery_task_outputs={"save_1": {"status": "succeeded"}},
        handler_data_snapshots={
            "save_1": {
                "task_id": "save_1",
                "function_external_id": "fn_dm_view_save",
                "handler_summary": {"instances_written": 2},
                "cohort_snapshot": {
                    "predecessor_cohort": {
                        "cohort_row_count": 1,
                        "truncated": False,
                        "cohort_rows": [{"key": "k1", "columns": {}}],
                    }
                },
            }
        },
        compiled_workflow={
            "tasks": [
                {"id": "save_1", "function_external_id": "fn_dm_view_save", "pipeline_node_id": "save"},
            ]
        },
    )
    assert doc["schema_version"] == DISCOVERY_RUN_SCHEMA_VERSION
    assert doc["pipeline"]["task_count"] == 2
    assert doc["persistence"]["node_count"] == 1
    node = doc["persistence"]["nodes"][0]
    assert node["kind"] == "view_save"
    assert node["input_cohort"]["entity_row_count"] == 1
    assert node["output"]["kind"] == "dm_instances_apply"
