"""Retry helpers for transient Cognite API failures (503, 429, etc.)."""

from __future__ import annotations

import time
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")

_TRANSIENT_HTTP_CODES = frozenset({429, 502, 503, 504})


def is_transient_cognite_error(exc: BaseException) -> bool:
    try:
        from cognite.client.exceptions import CogniteAPIError
    except ImportError:
        return False
    if not isinstance(exc, CogniteAPIError):
        return False
    code = getattr(exc, "code", None)
    if code in _TRANSIENT_HTTP_CODES:
        return True
    msg = str(exc).lower()
    return "service unavailable" in msg or "too many requests" in msg


def call_with_transient_retry(
    fn: Callable[[], T],
    *,
    max_attempts: int = 5,
    base_delay_sec: float = 1.0,
    max_delay_sec: float = 30.0,
    logger: Optional[Any] = None,
) -> T:
    """Call *fn*, retrying on transient Cognite API errors with exponential backoff."""
    delay = base_delay_sec
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max(1, max_attempts) + 1):
        try:
            return fn()
        except Exception as ex:
            last_exc = ex
            if not is_transient_cognite_error(ex) or attempt >= max_attempts:
                raise
            if logger is not None and hasattr(logger, "warning"):
                logger.warning(
                    "Transient Cognite API error (attempt %s/%s), retrying in %.1fs: %s",
                    attempt,
                    max_attempts,
                    delay,
                    ex,
                )
            time.sleep(delay)
            delay = min(delay * 2.0, max_delay_sec)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("call_with_transient_retry: no result")
