"""Thread-safe NDJSON progress lines for local runs (``KEA_UI_PROGRESS_FD``)."""

from __future__ import annotations

import json
import os
import threading
from typing import Any

_WRITE_LOCK = threading.Lock()


def emit_ui_progress_locked(event: str, **fields: Any) -> None:
    """Write one UTF-8 JSON line to the FD in ``KEA_UI_PROGRESS_FD`` if set and valid."""
    raw = (os.environ.get("KEA_UI_PROGRESS_FD") or "").strip()
    if not raw.isdigit():
        return
    fd = int(raw)
    line = json.dumps({"event": event, **fields}, ensure_ascii=False) + "\n"
    payload = line.encode("utf-8")
    with _WRITE_LOCK:
        try:
            os.write(fd, payload)
        except OSError:
            pass
