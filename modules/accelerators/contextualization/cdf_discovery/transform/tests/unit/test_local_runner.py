from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag  # noqa: E402
from local_runner.env import _REPO_ROOT  # noqa: E402
from local_runner.kahn_workflow_executor import run_compiled_workflow_dag  # noqa: E402
from local_runner.run import run_pipeline_document  # noqa: E402


def test_repo_root_resolves_above_transform_tree() -> None:
    discovery_root = ROOT.parent
    assert _REPO_ROOT == discovery_root.parent.parent.parent.parent


def test_run_pipeline_document_dry_run_minimal_canvas() -> None:
    doc = {
        "id": "test_local",
        "canvas": {
            "nodes": [
                {"id": "start", "kind": "start"},
                {
                    "id": "q1",
                    "kind": "query_view",
                    "data": {"config": {"view_external_id": "Asset"}},
                },
                {"id": "end", "kind": "end"},
            ],
            "edges": [
                {"source": "start", "target": "q1"},
                {"source": "q1", "target": "end"},
            ],
        },
    }
    payload = run_pipeline_document(doc, dry_run=True)
    assert payload["dry_run"] is True
    assert payload["run_id"]
    summaries = payload["task_summaries"]
    assert "q1" in summaries
    assert summaries["q1"].get("instances_written") == 0
    assert isinstance(summaries["q1"].get("duration_sec"), (int, float))
    assert summaries["q1"]["duration_sec"] >= 0


def test_disabled_canvas_node_skipped() -> None:
    canvas = {
        "nodes": [
            {"id": "q1", "kind": "query_view", "enabled": False, "data": {"config": {}}},
        ],
        "edges": [],
    }
    compiled = compile_canvas_dag(canvas)
    shared = {"configuration": {"canvas": canvas}}
    summaries = run_compiled_workflow_dag(
        compiled,
        client=None,
        logger=__import__("logging").getLogger("test"),
        shared_data=shared,
        dry_run=True,
    )
    assert summaries["q1"]["status"] == "skipped"
    assert summaries["q1"]["reason"] == "disabled"


def test_orchestration_task_skipped_locally() -> None:
    compiled = {
        "schema_version": 1,
        "tasks": [
            {
                "id": "sw1",
                "task_type": "subworkflow",
                "executable_kind": "subworkflow",
                "depends_on": [],
                "payload": {"config": {"workflow_external_id": "wf_other"}},
            }
        ],
    }
    summaries = run_compiled_workflow_dag(
        compiled,
        client=None,
        logger=__import__("logging").getLogger("test"),
        shared_data={"configuration": {}},
        dry_run=True,
    )
    assert summaries["sw1"]["reason"] == "orchestration_not_supported_locally"
