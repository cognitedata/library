"""Unit tests for monitor workflow-state API helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ui.server import monitor_api


def _make_execution(
    *,
    run_id: str,
    workflow_id: str,
    status: str,
    start_time: str = "2026-05-28T10:00:00+00:00",
    end_time: str = "2026-05-28T10:05:00+00:00",
):
    ex = MagicMock()
    ex.dump.return_value = {
        "id": run_id,
        "workflow_external_id": workflow_id,
        "workflow_version": "v1",
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
    }
    return ex


def test_normalize_cdf_execution_basic_fields():
    execution = _make_execution(run_id="run-1", workflow_id="wf_a", status="completed")
    out = monitor_api._normalize_cdf_execution(execution)
    assert out["source"] == "cdf"
    assert out["run_id"] == "run-1"
    assert out["workflow_id"] == "wf_a"
    assert out["status"] == "succeeded"
    assert out["duration_ms"] == 300000


def test_build_workflow_rows_merges_local_and_cdf():
    client = MagicMock()
    client.workflows.executions.list.return_value = [
        _make_execution(run_id="run-1", workflow_id="wf_a", status="failed"),
        _make_execution(run_id="run-2", workflow_id="wf_a", status="running"),
    ]
    with (
        patch(
            "ui.server.monitor_api.cdf_browse.list_workflows",
            return_value=[{"external_id": "wf_a", "label": "Workflow A"}],
        ),
        patch(
            "ui.server.monitor_api.transform_registry.list_pipeline_tree_entries",
            return_value=[{"id": "pipe_1", "label": "Pipeline 1", "scope_suffix": ""}],
        ),
        patch(
            "ui.server.monitor_api.transform_registry.pipeline_build_pairing",
            return_value={"workflow_external_id": "wf_a"},
        ),
    ):
        rows, runs = monitor_api._build_workflow_rows(client, run_limit=50)
    assert len(rows) == 1
    assert len(runs) == 2
    row = rows[0]
    assert row["workflow_id"] == "wf_a"
    assert row["run_count"] == 2
    assert row["failed_count"] == 1
    assert row["running_count"] == 1
    assert row["degraded"] is True
    assert "local" in row["sources"]
    assert "cdf" in row["sources"]


def test_workflow_state_summary_counts_statuses():
    with patch("ui.server.monitor_api._cdf_client", return_value=MagicMock()), patch(
        "ui.server.monitor_api._build_workflow_rows",
        return_value=(
            [
                {"latest_status": "running", "degraded": False},
                {"latest_status": "succeeded", "degraded": False},
                {"latest_status": "failed", "degraded": True},
            ],
            [{"run_id": "1"}, {"run_id": "2"}],
        ),
    ):
        out = monitor_api.workflow_state_summary(run_limit=200)
    assert out["workflow_count"] == 3
    assert out["run_count"] == 2
    assert out["running_workflows"] == 1
    assert out["succeeded_workflows"] == 1
    assert out["failed_workflows"] == 1
    assert out["degraded_workflows"] == 1


def test_workflow_state_workflow_detail_returns_task_status_counts():
    client = MagicMock()
    brief = _make_execution(run_id="run-1", workflow_id="wf_a", status="failed")
    detailed = MagicMock()
    detailed.dump.return_value = {
        "id": "run-1",
        "workflow_external_id": "wf_a",
        "status": "failed",
        "start_time": "2026-05-28T10:00:00+00:00",
        "end_time": "2026-05-28T10:05:00+00:00",
        "tasks": [
            {"id": "task1", "status": "failed"},
            {"id": "task2", "status": "completed"},
        ],
    }
    client.workflows.executions.list.return_value = [brief]
    client.workflows.executions.retrieve_detailed.return_value = detailed
    with patch("ui.server.monitor_api._cdf_client", return_value=client), patch(
        "ui.server.monitor_api._build_workflow_rows",
        return_value=([{"workflow_id": "wf_a", "label": "Workflow A"}], []),
    ):
        out = monitor_api.workflow_state_workflow_detail("wf_a", runs_limit=5)
    assert out["workflow"]["workflow_id"] == "wf_a"
    assert len(out["runs"]) == 1
    assert out["runs"][0]["failed_tasks"] == 1
    assert out["task_status_counts"]["failed"] == 1
    assert out["task_status_counts"]["succeeded"] == 1


def test_monitor_schedules_includes_pipeline_and_avg_runtime():
    client = MagicMock()
    client.workflows.executions.list.return_value = [
        _make_execution(
            run_id="run-1",
            workflow_id="wf_pipeline",
            status="completed",
            start_time="2026-05-29T10:00:00+00:00",
            end_time="2026-05-29T10:10:00+00:00",
        )
    ]
    trigger = MagicMock()
    trigger.dump.return_value = {
        "external_id": "trg_pipeline",
        "workflow_external_id": "wf_pipeline",
        "workflow_version": "v1",
        "trigger_rule": {"cronExpression": "0 6 * * *"},
    }
    client.workflows.triggers.list.return_value = [trigger]
    with (
        patch("ui.server.monitor_api._cdf_client", return_value=client),
        patch(
            "ui.server.monitor_api.transform_registry.list_pipeline_tree_entries",
            return_value=[{"id": "pipe_a", "label": "Pipeline A", "scope_suffix": ""}],
        ),
        patch(
            "ui.server.monitor_api.transform_registry.pipeline_build_pairing",
            return_value={"workflow_external_id": "wf_pipeline"},
        ),
    ):
        out = monitor_api.monitor_schedules(lookback_days=7, executions_limit=50, trigger_limit=50)
    assert out["lookback_days"] == 7
    assert len(out["schedules"]) == 1
    row = out["schedules"][0]
    assert row["entity_type"] == "pipeline"
    assert row["entity_label"] == "Pipeline A"
    assert row["cron_expression"] == "0 6 * * *"
    assert row["avg_runtime_ms_7d"] == 600000
