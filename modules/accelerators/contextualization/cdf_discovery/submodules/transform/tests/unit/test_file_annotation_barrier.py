"""Unit tests for file-annotation completion barrier deferral."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_TRANSFORM_ROOT = Path(__file__).resolve().parents[2]
_FUNCTIONS = _TRANSFORM_ROOT / "functions"
_SHARED = _TRANSFORM_ROOT.parent.parent / "shared"
for p in (_TRANSFORM_ROOT, _FUNCTIONS, _SHARED):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from cdf_fn_common.etl_file_annotation_async_orchestration import (  # noqa: E402
    etl_handle_file_annotation_barrier,
)


@patch("cdf_fn_common.etl_file_annotation_async_orchestration.write_file_annotation_cohort_rows")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration._poll_and_process_detect_jobs")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration._load_queue_detect_jobs")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration.file_state_sink_from_data")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration.resolve_file_workflow_params")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration.require_pipeline_run_key")
def test_barrier_defers_when_jobs_still_pending(
    mock_run_id: MagicMock,
    mock_params: MagicMock,
    mock_sink: MagicMock,
    mock_load_jobs: MagicMock,
    mock_poll: MagicMock,
    _mock_write: MagicMock,
) -> None:
    mock_run_id.return_value = "run-1"
    mock_params.return_value = {"workflow_scope": "scope-a"}
    mock_sink.return_value = ("db", "state")
    pending_job = {
        "task_id": "fanout",
        "pack_index": 0,
        "job_id": 123,
        "job_status": "distributed",
    }
    mock_load_jobs.return_value = [pending_job]
    mock_poll.return_value = (0, [pending_job])

    client = MagicMock()
    data = {
        "task_id": "completion_barrier",
        "run_id": "run-1",
        "config": {"fanout_mode": "both", "source_task_id": "fanout"},
        "configuration": {"parameters": {"fanout_mode": "both"}},
    }
    summary = etl_handle_file_annotation_barrier(
        "fn_discovery_etl_file_annotation_barrier",
        data,
        client,
        None,
    )
    assert summary["status"] == "deferred"
    assert summary["jobs_pending"] == 1
    client.workflows.tasks.complete.assert_not_called()


@patch("cdf_fn_common.etl_file_annotation_async_orchestration.complete_workflow_task")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration.write_file_annotation_cohort_rows")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration._poll_and_process_detect_jobs")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration._load_queue_detect_jobs")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration.file_state_sink_from_data")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration.resolve_file_workflow_params")
@patch("cdf_fn_common.etl_file_annotation_async_orchestration.require_pipeline_run_key")
def test_barrier_completes_when_all_jobs_terminal(
    mock_run_id: MagicMock,
    mock_params: MagicMock,
    mock_sink: MagicMock,
    mock_load_jobs: MagicMock,
    mock_poll: MagicMock,
    _mock_write: MagicMock,
    mock_complete: MagicMock,
) -> None:
    mock_run_id.return_value = "run-1"
    mock_params.return_value = {"workflow_scope": "scope-a"}
    mock_sink.return_value = ("db", "state")
    terminal_job = {
        "task_id": "fanout",
        "pack_index": 0,
        "job_id": 123,
        "job_status": "completed_processed",
    }
    mock_load_jobs.return_value = [terminal_job]
    mock_poll.return_value = (0, [])

    client = MagicMock()
    data = {
        "task_id": "completion_barrier",
        "run_id": "run-1",
        "cogniteOrchestrationTaskId": 99,
        "config": {"fanout_mode": "both", "source_task_id": "fanout"},
        "configuration": {"parameters": {"fanout_mode": "both"}},
    }
    summary = etl_handle_file_annotation_barrier(
        "fn_discovery_etl_file_annotation_barrier",
        data,
        client,
        None,
    )
    assert summary["status"] == "ok"
    mock_complete.assert_called_once()
