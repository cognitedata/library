"""NDJSON streaming helpers for long-running local operations."""

from __future__ import annotations

import asyncio
import json
import queue
import sys
import threading
from collections.abc import Callable
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse

from inverted_index.cancellation import OperationCancelled
from local_runner.log_capture import capture_operation_logs

RunFn = Callable[[Callable[[str], None] | None, Callable[[], bool]], Any]


def _stream_not_supported() -> None:
    if sys.platform == "win32":
        raise HTTPException(
            status_code=501,
            detail="Progress streaming uses a background thread; use the non-streaming endpoint on Windows.",
        )


def stream_operation(run: RunFn, request: Request) -> StreamingResponse:
    """Run ``run(on_log, should_cancel)`` in a worker thread and stream NDJSON events."""
    _stream_not_supported()

    log_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
    cancel_event = threading.Event()

    def on_log(line: str) -> None:
        log_queue.put(("log", line))

    def should_cancel() -> bool:
        return cancel_event.is_set()

    def worker() -> None:
        try:
            with capture_operation_logs(on_log):
                result = run(on_log, should_cancel)
            log_queue.put(("result", result))
        except OperationCancelled:
            log_queue.put(("log", "[operation] cancelled"))
            log_queue.put(("result", {"cancelled": True}))
        except Exception as exc:
            log_queue.put(("error", str(exc)))

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    async def ndjson_iter():
        exit_code = 0
        disconnected = False
        while True:
            if await request.is_disconnected():
                cancel_event.set()
                disconnected = True
            if not thread.is_alive() and log_queue.empty():
                break
            try:
                kind, payload = log_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue
            if kind == "log":
                yield json.dumps({"event": "log", "line": payload}, ensure_ascii=False) + "\n"
            elif kind == "result":
                yield (
                    json.dumps({"event": "result", "data": payload}, ensure_ascii=False, default=str)
                    + "\n"
                )
            elif kind == "error":
                exit_code = 1
                yield json.dumps({"event": "error", "detail": payload}, ensure_ascii=False) + "\n"
        if disconnected:
            thread.join(timeout=2.0)
        else:
            thread.join()
        yield json.dumps({"event": "exit", "code": exit_code}, ensure_ascii=False) + "\n"

    return StreamingResponse(ndjson_iter(), media_type="application/x-ndjson")
