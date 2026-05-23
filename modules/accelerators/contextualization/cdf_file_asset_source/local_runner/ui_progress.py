"""Optional NDJSON progress lines for the operator UI (``FAS_UI_PROGRESS_FD`` env)."""

from __future__ import annotations

import io
import json
import logging
import os
import sys
from contextlib import contextmanager
from typing import Any, Iterator, TextIO

_MAX_LOG_CHARS = 8000


def ui_progress_fd_configured() -> bool:
    raw = (os.environ.get("FAS_UI_PROGRESS_FD") or "").strip()
    return raw.isdigit()


def emit_ui_progress(event: str, **fields: Any) -> None:
    """Write one UTF-8 JSON line to the FD in ``FAS_UI_PROGRESS_FD`` if set and valid."""
    raw = (os.environ.get("FAS_UI_PROGRESS_FD") or "").strip()
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


class _UiProgressStream(io.TextIOBase):
    """Tee writes to the real stream and emit ``log`` NDJSON lines when the progress FD is set."""

    def __init__(self, stream: TextIO, level: str) -> None:
        self._stream = stream
        self._level = level
        self._pending = ""

    def write(self, s: str) -> int:
        if not s:
            return 0
        self._stream.write(s)
        self._stream.flush()
        if ui_progress_fd_configured():
            self._pending += s
            while "\n" in self._pending:
                line, self._pending = self._pending.split("\n", 1)
                text = line.rstrip("\r")
                if text:
                    emit_ui_progress("log", level=self._level, message=text)
        return len(s)

    def flush(self) -> None:
        self._stream.flush()
        if ui_progress_fd_configured() and self._pending.strip():
            emit_ui_progress("log", level=self._level, message=self._pending.rstrip("\r"))
            self._pending = ""

    def isatty(self) -> bool:
        return self._stream.isatty()


@contextmanager
def ui_progress_stdio_forwarding() -> Iterator[None]:
    """
    Tee ``sys.stdout`` / ``sys.stderr`` into NDJSON ``log`` events.

    Pipelines use :class:`~fn_dm_extract_assets_by_pattern.logger.CogniteFunctionLogger` (``print``),
    not the stdlib ``logging`` module.
    """
    if not ui_progress_fd_configured():
        yield
        return
    old_out, old_err = sys.stdout, sys.stderr
    sys.stdout = _UiProgressStream(old_out, "STDOUT")  # type: ignore[assignment]
    sys.stderr = _UiProgressStream(old_err, "STDERR")  # type: ignore[assignment]
    try:
        yield
    finally:
        try:
            sys.stdout.flush()  # type: ignore[union-attr]
        except Exception:
            pass
        try:
            sys.stderr.flush()  # type: ignore[union-attr]
        except Exception:
            pass
        sys.stdout = old_out
        sys.stderr = old_err


@contextmanager
def ui_progress_log_forwarding(min_level: int = logging.INFO) -> Iterator[None]:
    """
    Attach :class:`UiProgressLoggingHandler` to the root logger while the UI progress FD is set.

    When ``FAS_UI_PROGRESS_FD`` is unset, this is a no-op context manager.
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
