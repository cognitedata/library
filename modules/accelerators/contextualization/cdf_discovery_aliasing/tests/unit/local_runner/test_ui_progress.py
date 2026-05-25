"""Tests for ``local_runner.ui_progress``."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

# Import ``local_runner`` from the module root without loading ``cdf_discovery_aliasing`` package ``__init__``.
_MOD_ROOT = Path(__file__).resolve().parents[3]
if str(_MOD_ROOT) not in sys.path:
    sys.path.insert(0, str(_MOD_ROOT))

from local_runner.ui_progress import (
    emit_ui_progress,
    resolve_task_read_count,
    ui_progress_log_forwarding,
    ui_progress_row_counts,
    ui_progress_row_counts_from_output,
)


def test_emit_ui_progress_noop_without_env() -> None:
    os.environ.pop("KEA_UI_PROGRESS_FD", None)
    emit_ui_progress("task_start", task_id="x")  # should not raise


def test_emit_ui_progress_writes_json_line() -> None:
    r_fd, w_fd = os.pipe()
    os.environ["KEA_UI_PROGRESS_FD"] = str(w_fd)
    try:
        emit_ui_progress("task_start", task_id="incremental_state")
        os.close(w_fd)
        with os.fdopen(r_fd, "r", encoding="utf-8") as rf:
            line = rf.readline()
    finally:
        os.environ.pop("KEA_UI_PROGRESS_FD", None)
    data = json.loads(line)
    assert data == {"event": "task_start", "task_id": "incremental_state"}


def test_ui_progress_log_forwarding_emits_log_event() -> None:
    r_fd, w_fd = os.pipe()
    os.environ["KEA_UI_PROGRESS_FD"] = str(w_fd)
    try:
        with ui_progress_log_forwarding(logging.INFO):
            logging.getLogger("test_ui_progress_fwd").info("hello from handler")
        os.close(w_fd)
        raw = os.read(r_fd, 65536).decode("utf-8")
    finally:
        os.environ.pop("KEA_UI_PROGRESS_FD", None)
        try:
            os.close(r_fd)
        except OSError:
            pass
    lines = [ln for ln in raw.split("\n") if ln.strip()]
    assert len(lines) >= 1
    last = json.loads(lines[-1])
    assert last["event"] == "log"
    assert last["level"] == "INFO"
    assert "hello from handler" in last["message"]


def test_ui_progress_row_counts_from_output_parses_message_json() -> None:
    counts = ui_progress_row_counts_from_output(
        {"status": "succeeded", "message": '{"rows_read": 5, "rows_written": 4}'}
    )
    assert counts == {"rows_read": 5, "rows_written": 4}


def test_ui_progress_row_counts_prefers_nonzero_instances_listed() -> None:
    assert resolve_task_read_count({"rows_read": 0, "instances_listed": 12}) == 12
    assert ui_progress_row_counts({"rows_read": 0, "instances_listed": 12}) == {"rows_read": 12}


def test_ui_progress_row_counts_ignores_invalid() -> None:
    assert ui_progress_row_counts({"rows_read": "nope"}) == {}
