"""Tests for ``local_runner.ui_progress`` row count extraction."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from local_runner.ui_progress import (  # noqa: E402
    resolve_task_read_count,
    resolve_task_write_count,
    ui_progress_row_counts,
)


def test_ui_progress_row_counts_extracts_numeric_fields() -> None:
    assert ui_progress_row_counts(
        {"rows_read": 10, "rows_written": 8, "status": "ok"}
    ) == {"rows_read": 10, "rows_written": 8}


def test_ui_progress_row_counts_prefers_nonzero_instances_listed() -> None:
    assert resolve_task_read_count({"rows_read": 0, "instances_listed": 42}) == 42
    assert ui_progress_row_counts({"rows_read": 0, "instances_listed": 42}) == {
        "rows_read": 42,
    }


def test_ui_progress_row_counts_prefers_nonzero_instances_written() -> None:
    assert resolve_task_write_count({"rows_written": 0, "instances_written": 7}) == 7


def test_ui_progress_row_counts_ignores_non_numeric() -> None:
    assert ui_progress_row_counts({"rows_read": "10"}) == {}
