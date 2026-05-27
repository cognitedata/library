"""Tests for ``cdf_fn_common.etl_ui_progress``."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
FN_ROOT = ROOT / "functions"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(FN_ROOT) not in sys.path:
    sys.path.insert(0, str(FN_ROOT))

from cdf_fn_common import etl_ui_progress  # noqa: E402


def test_emit_handler_progress_writes_task_progress(monkeypatch) -> None:
    lines: list[str] = []

    def fake_write(fd: int, data: bytes) -> int:
        lines.append(data.decode("utf-8"))
        return len(data)

    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    monkeypatch.setenv("KEA_UI_PROGRESS_FD", str(write_fd))
    monkeypatch.setattr(etl_ui_progress.os, "write", fake_write)

    etl_ui_progress.bind_handler_progress(
        {
            "task_id": "task_a",
            "compiled_task": {
                "canvas_node_id": "query_view_1",
                "function_external_id": "fn_etl_view_query",
            },
        }
    )
    etl_ui_progress.emit_handler_progress(5, total=10, label="instances", force=True)
    etl_ui_progress.clear_handler_progress()
    os.close(write_fd)

    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "task_progress"
    assert payload["task_id"] == "task_a"
    assert payload["canvas_node_id"] == "query_view_1"
    assert payload["progress_current"] == 5
    assert payload["progress_total"] == 10
    assert payload["progress_label"] == "instances"


def test_emit_handler_progress_noop_without_fd(monkeypatch) -> None:
    monkeypatch.delenv("KEA_UI_PROGRESS_FD", raising=False)
    etl_ui_progress.bind_handler_progress({"task_id": "t1"})
    etl_ui_progress.emit_handler_progress(1, force=True)
    etl_ui_progress.clear_handler_progress()
