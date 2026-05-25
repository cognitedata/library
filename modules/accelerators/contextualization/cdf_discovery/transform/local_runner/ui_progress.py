"""Optional NDJSON progress lines for the operator UI (``KEA_UI_PROGRESS_FD`` env)."""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator, Mapping

_MAX_LOG_CHARS = 8000


def _int_field(summary: Mapping[str, Any], key: str) -> int | None:
    val = summary.get(key)
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        return None
    return int(val)


def resolve_task_read_count(summary: Mapping[str, Any]) -> int | None:
    """Best read count from handler summaries (prefer non-zero over stale ``rows_read: 0``)."""
    vals: list[int] = []
    for key in ("rows_read", "instances_listed"):
        v = _int_field(summary, key)
        if v is not None:
            vals.append(v)
    left = _int_field(summary, "rows_read_left")
    right = _int_field(summary, "rows_read_right")
    if left is not None or right is not None:
        vals.append((left or 0) + (right or 0))
    if not vals:
        return None
    positive = [v for v in vals if v > 0]
    return max(positive) if positive else vals[0]


def resolve_task_write_count(summary: Mapping[str, Any]) -> int | None:
    """Best write count from handler summaries."""
    vals: list[int] = []
    for key in ("rows_written", "instances_written", "instances_applied", "row_count"):
        v = _int_field(summary, key)
        if v is not None:
            vals.append(v)
    if not vals:
        return None
    positive = [v for v in vals if v > 0]
    return max(positive) if positive else vals[0]


def ui_progress_row_counts(summary: Mapping[str, Any] | None) -> dict[str, int]:
    """Canonical read/write counts for ``task_end`` NDJSON events."""
    if not isinstance(summary, Mapping):
        return {}
    out: dict[str, int] = {}
    read = resolve_task_read_count(summary)
    write = resolve_task_write_count(summary)
    if read is not None:
        out["rows_read"] = read
    if write is not None:
        out["rows_written"] = write
    return out


def ui_progress_fd_configured() -> bool:
    raw = (os.environ.get("KEA_UI_PROGRESS_FD") or "").strip()
    return raw.isdigit()


def emit_ui_progress(event: str, **fields: Any) -> None:
    """Write one UTF-8 JSON line to the FD in ``KEA_UI_PROGRESS_FD`` if set and valid."""
    raw = (os.environ.get("KEA_UI_PROGRESS_FD") or "").strip()
    if not raw.isdigit():
        return
    fd = int(raw)
    try:
        line = json.dumps({"event": event, **fields}, ensure_ascii=False) + "\n"
        os.write(fd, line.encode("utf-8"))
    except OSError:
        pass


class UiProgressLoggingHandler(logging.Handler):
    """Forward log records as ``{"event": "log", "level", "message"}`` NDJSON lines."""

    def __init__(self, level: int = logging.INFO) -> None:
        super().__init__(level)
        self.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            if len(msg) > _MAX_LOG_CHARS:
                msg = msg[:_MAX_LOG_CHARS] + "…"
            emit_ui_progress("log", level=record.levelname, message=msg)
        except Exception:
            self.handleError(record)


@contextmanager
def ui_progress_log_forwarding(min_level: int = logging.INFO) -> Iterator[None]:
    """
    Attach :class:`UiProgressLoggingHandler` to the root logger while the UI progress FD is set.

    When ``KEA_UI_PROGRESS_FD`` is unset, this is a no-op context manager.
    """
    if not ui_progress_fd_configured():
        yield
        return
    handler = UiProgressLoggingHandler(min_level)
    root = logging.getLogger()
    prev_level = root.level
    lowered = False
    if root.level > min_level:
        root.setLevel(min_level)
        lowered = True
    root.addHandler(handler)
    try:
        yield
    finally:
        root.removeHandler(handler)
        if lowered:
            root.setLevel(prev_level)
