"""Tests for workflow task onFailure resolution and local DAG behavior."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
FUNCS = ROOT / "functions"
for p in (str(ROOT), str(FUNCS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from cdf_fn_common.workflow_compile.canvas_dag import compile_canvas_dag  # noqa: E402
from cdf_fn_common.workflow_compile.codegen import build_workflow_version_document  # noqa: E402
from cdf_fn_common.workflow_task_failure import resolve_task_on_failure  # noqa: E402
from cdf_fn_common.workflow_task_policy import discovery_task_workflow_policy  # noqa: E402
from local_runner.kahn_workflow_executor import run_compiled_workflow_dag  # noqa: E402


def test_default_function_policy_is_skip_task() -> None:
    pol = discovery_task_workflow_policy("fn_etl_view_query")
    assert pol["onFailure"] == "abortWorkflow"


def test_fanout_plan_still_aborts_workflow() -> None:
    pol = discovery_task_workflow_policy("fn_etl_workflow_fanout_plan")
    assert pol["onFailure"] == "abortWorkflow"


def test_cdf_task_canvas_defaults_on_failure() -> None:
    compiled = compile_canvas_dag(
        {
            "nodes": [
                {"id": "t1", "kind": "cdf_task", "data": {"config": {"description": "x"}}},
            ],
            "edges": [],
        }
    )
    task = next(t for t in compiled["tasks"] if t["id"] == "t1")
    assert task["on_failure"] == "skipTask"


def test_codegen_uses_canvas_on_failure() -> None:
    compiled = compile_canvas_dag(
        {
            "nodes": [
                {
                    "id": "t1",
                    "kind": "cdf_task",
                    "data": {"config": {"on_failure": "abortWorkflow"}},
                },
            ],
            "edges": [],
        }
    )
    wv = build_workflow_version_document(
        workflow_external_id="wf_test",
        version="1",
        compiled_workflow=compiled,
    )
    task = wv["workflowDefinition"]["tasks"][0]
    assert task["onFailure"] == "abortWorkflow"


def test_local_dag_continues_after_skip_task_failure() -> None:
    compiled = {
        "tasks": [
            {
                "id": "a",
                "task_type": "function",
                "function_external_id": "fn_missing",
                "depends_on": [],
                "on_failure": "skipTask",
            },
            {
                "id": "b",
                "task_type": "function",
                "function_external_id": "fn_missing",
                "depends_on": ["a"],
                "on_failure": "skipTask",
            },
        ]
    }
    shared: dict = {"configuration": {}, "compiled_workflow": compiled}

    def _fake_run(work, **kwargs):
        from local_runner.kahn_workflow_executor import _LayerTaskResult

        return _LayerTaskResult(
            task_id=work.task_id,
            summary={"status": "failed", "task_id": work.task_id, "error": "boom"},
            task_data=None,
            in_memory=False,
        )

    with patch(
        "local_runner.kahn_workflow_executor._run_single_compiled_task",
        side_effect=_fake_run,
    ):
        summaries = run_compiled_workflow_dag(
            compiled,
            client=None,
            logger=__import__("logging").getLogger("test"),
            shared_data=shared,
            dry_run=True,
        )
    assert "a" in summaries and "b" in summaries
    assert summaries["a"]["status"] == "failed"
    assert summaries["b"]["status"] == "failed"


def test_resolve_task_on_failure_prefers_explicit() -> None:
    task = {"on_failure": "abortWorkflow", "task_type": "cdf"}
    assert resolve_task_on_failure(task) == "abortWorkflow"
