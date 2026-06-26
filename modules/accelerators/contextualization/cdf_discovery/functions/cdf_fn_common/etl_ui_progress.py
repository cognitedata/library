"""Emit ``task_progress`` NDJSON for handler loops (``KEA_UI_PROGRESS_FD`` env, local runs only)."""

from __future__ import annotations

import os
import time
from contextvars import ContextVar
from typing import Any, Mapping

try:
    from local_runner.progress_writer import emit_ui_progress_locked
except ImportError:
    import json
    import threading

    _FALLBACK_LOCK = threading.Lock()

    def emit_ui_progress_locked(event: str, **fields: Any) -> None:
        raw = (os.environ.get("KEA_UI_PROGRESS_FD") or "").strip()
        if not raw.isdigit():
            return
        fd = int(raw)
        line = json.dumps({"event": event, **fields}, ensure_ascii=False) + "\n"
        with _FALLBACK_LOCK:
            try:
                os.write(fd, line.encode("utf-8"))
            except OSError:
                pass

_THROTTLE_SEC = 0.2
HANDLER_PROGRESS_ROW_INTERVAL = 500
COHORT_WRITE_ROW_INTERVAL = 10_000
_ctx: ContextVar[dict[str, Any] | None] = ContextVar("etl_handler_progress", default=None)
_cohort_write_total: ContextVar[int | None] = ContextVar("etl_cohort_write_total", default=None)
_last_emit: ContextVar[float] = ContextVar("etl_handler_progress_last_emit", default=0.0)


def ui_progress_fd_configured() -> bool:
    raw = (os.environ.get("KEA_UI_PROGRESS_FD") or "").strip()
    return raw.isdigit()


def _fields_from_data(data: Mapping[str, Any]) -> dict[str, Any]:
    task_id = str(data.get("task_id") or "").strip()
    compiled = data.get("compiled_task")
    canvas = ""
    fn_ext = ""
    pnode = ""
    if isinstance(compiled, Mapping):
        canvas = str(compiled.get("canvas_node_id") or "").strip()
        fn_ext = str(compiled.get("function_external_id") or "").strip()
        pnode = str(compiled.get("pipeline_node_id") or "").strip()
    out: dict[str, Any] = {}
    if task_id:
        out["task_id"] = task_id
    if fn_ext:
        out["function_external_id"] = fn_ext
    if canvas:
        out["canvas_node_id"] = canvas
    if pnode and pnode != task_id:
        out["pipeline_node_id"] = pnode
    return out


def bind_handler_progress(data: Mapping[str, Any]) -> None:
    if not ui_progress_fd_configured():
        return
    _ctx.set(_fields_from_data(data))
    _cohort_write_total.set(None)
    _last_emit.set(0.0)


def clear_handler_progress() -> None:
    _ctx.set(None)
    _cohort_write_total.set(None)


def set_cohort_write_progress_total(total: int | None) -> None:
    """When set, cohort write progress events include ``progress_total`` (n/total on canvas)."""
    if total is not None and int(total) > 0:
        _cohort_write_total.set(int(total))
    else:
        _cohort_write_total.set(None)


def _cohort_write_progress_total(rows_written: int, *, finalize: bool = False) -> int | None:
    bound = _cohort_write_total.get()
    if bound is not None and bound > 0:
        return bound
    if finalize and rows_written > 0:
        return rows_written
    return None


def emit_handler_progress(
    current: int,
    *,
    total: int | None = None,
    label: str | None = None,
    force: bool = False,
) -> None:
    ctx = _ctx.get()
    if not ctx:
        return
    now = time.monotonic()
    last = _last_emit.get()
    at_complete = total is not None and total > 0 and current >= total
    if not force and not at_complete and last and (now - last) < _THROTTLE_SEC:
        return
    fields: dict[str, Any] = dict(ctx)
    fields["progress_current"] = max(0, int(current))
    if total is not None and int(total) > 0:
        fields["progress_total"] = int(total)
    if label:
        fields["progress_label"] = str(label)[:200]
    emit_ui_progress_locked("task_progress", **fields)
    _last_emit.set(now)


def emit_handler_progress_every_n_rows(
    rows_processed: int,
    *,
    interval: int = HANDLER_PROGRESS_ROW_INTERVAL,
    label: str = "rows",
) -> None:
    if rows_processed > 0 and rows_processed % interval == 0:
        emit_handler_progress(rows_processed, label=label)


def emit_handler_progress_rows_complete(
    rows_processed: int,
    *,
    label: str = "rows",
) -> None:
    if rows_processed > 0:
        emit_handler_progress(rows_processed, label=label, force=True)


def emit_cohort_write_progress_every_n_rows(
    rows_written: int,
    *,
    interval: int = COHORT_WRITE_ROW_INTERVAL,
) -> None:
    """Emit ``task_progress`` for the active canvas node every *interval* cohort RAW rows."""
    if rows_written > 0 and rows_written % interval == 0:
        emit_handler_progress(
            rows_written,
            total=_cohort_write_progress_total(rows_written, finalize=False),
            label="cohort_rows",
        )


def emit_cohort_write_progress_complete(rows_written: int) -> None:
    if rows_written > 0:
        emit_handler_progress(
            rows_written,
            total=_cohort_write_progress_total(rows_written, finalize=True),
            label="cohort_rows",
            force=True,
        )
