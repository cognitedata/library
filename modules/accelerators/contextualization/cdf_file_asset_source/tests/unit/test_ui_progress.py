"""Unit tests for local_runner UI progress emission."""

import json
import os

import sys

from local_runner.ui_progress import emit_ui_progress, ui_progress_stdio_forwarding


def test_emit_ui_progress_noop_without_fd(monkeypatch):
    monkeypatch.delenv("FAS_UI_PROGRESS_FD", raising=False)
    emit_ui_progress("task_start", task_id="extract")  # should not raise


def test_emit_ui_progress_writes_ndjson(monkeypatch):
    r_fd, w_fd = os.pipe()
    try:
        monkeypatch.setenv("FAS_UI_PROGRESS_FD", str(w_fd))
        emit_ui_progress("task_start", task_id="create", function_external_id="fn_dm_create_asset_hierarchy")
        os.close(w_fd)
        with os.fdopen(r_fd, "r", encoding="utf-8") as rf:
            line = rf.readline()
        data = json.loads(line)
        assert data["event"] == "task_start"
        assert data["task_id"] == "create"
    finally:
        monkeypatch.delenv("FAS_UI_PROGRESS_FD", raising=False)
        try:
            os.close(w_fd)
        except OSError:
            pass
        try:
            os.close(r_fd)
        except OSError:
            pass


def test_ui_progress_stdio_forwarding_emits_log_event(monkeypatch):
    r_fd, w_fd = os.pipe()
    try:
        monkeypatch.setenv("FAS_UI_PROGRESS_FD", str(w_fd))
        with ui_progress_stdio_forwarding():
            print("hello from pipeline")
        os.close(w_fd)
        with os.fdopen(r_fd, "r", encoding="utf-8") as rf:
            lines = [json.loads(ln) for ln in rf if ln.strip()]
        log_events = [e for e in lines if e.get("event") == "log"]
        assert any("hello from pipeline" in e.get("message", "") for e in log_events)
    finally:
        monkeypatch.delenv("FAS_UI_PROGRESS_FD", raising=False)
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        try:
            os.close(w_fd)
        except OSError:
            pass
        try:
            os.close(r_fd)
        except OSError:
            pass
