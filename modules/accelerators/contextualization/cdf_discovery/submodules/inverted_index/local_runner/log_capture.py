"""Capture stderr and logging output during local operation runs."""

from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from typing import Callable, Iterator


class _TeeStderr:
    def __init__(self, original: object, on_line: Callable[[str], None] | None) -> None:
        self._original = original
        self._on_line = on_line
        self._buffer = ""

    def write(self, text: str) -> int:
        self._original.write(text)
        if not text:
            return 0
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if self._on_line and line:
                self._on_line(line)
        return len(text)

    def flush(self) -> None:
        self._original.flush()
        if self._buffer and self._on_line:
            self._on_line(self._buffer)
            self._buffer = ""

    def __getattr__(self, name: str) -> object:
        return getattr(self._original, name)


class _QueueLogHandler(logging.Handler):
    def __init__(self, on_line: Callable[[str], None]) -> None:
        super().__init__()
        self._on_line = on_line

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:
            self.handleError(record)
            return
        if message:
            self._on_line(message)


@contextmanager
def tee_stderr(on_line: Callable[[str], None] | None = None) -> Iterator[None]:
    original = sys.stderr
    sys.stderr = _TeeStderr(original, on_line)
    try:
        yield
    finally:
        sys.stderr.flush()
        sys.stderr = original


@contextmanager
def capture_operation_logs(on_line: Callable[[str], None] | None = None) -> Iterator[None]:
    """Mirror stderr and INFO+ log records to ``on_line`` while active."""
    handler: _QueueLogHandler | None = None
    root = logging.getLogger()
    if on_line is not None:
        handler = _QueueLogHandler(on_line)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        root.addHandler(handler)
    try:
        with tee_stderr(on_line):
            yield
    finally:
        if handler is not None:
            root.removeHandler(handler)
