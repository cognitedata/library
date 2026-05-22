"""Unified discovery Cognite Function success/failure contract (CDF workflows + local runner)."""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, MutableMapping, NoReturn, Optional

STATUS_SUCCEEDED = "succeeded"
STATUS_FAILURE = "failure"


class DiscoveryPipelineError(Exception):
    """Fatal pipeline error: Cognite Function invocation should fail; workflow may retry."""


def discovery_handler_success(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    summary: Any,
    *,
    log: Optional[Any] = None,
    default=str,
) -> Dict[str, Any]:
    """Set ``data`` status/message and return Cognite-style success response."""
    msg = json.dumps(summary, default=default)
    data["status"] = STATUS_SUCCEEDED
    data["message"] = msg
    if log is not None and hasattr(log, "info"):
        log.info("%s complete", fn_external_id)
    return {"status": STATUS_SUCCEEDED, "message": msg}


def discovery_handler_failure(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    message: str,
    *,
    log: Optional[Any] = None,
    cause: Optional[BaseException] = None,
) -> NoReturn:
    """Log, set ``data`` status/message, and raise ``DiscoveryPipelineError``."""
    full = f"{fn_external_id} failed: {message}"
    data["status"] = STATUS_FAILURE
    data["message"] = full
    if log is not None and hasattr(log, "error"):
        log.error(full)
    if cause is not None:
        raise DiscoveryPipelineError(full) from cause
    raise DiscoveryPipelineError(full)


def apply_handler_output(
    out: Any,
    data: MutableMapping[str, Any],
    *,
    raise_on_failure: bool = True,
) -> Dict[str, Any]:
    """
    Copy handler return dict onto ``data``; optionally raise on logical failure.

    Used by local ``pipeline.py`` entrypoints for defense in depth.
    """
    if not isinstance(out, dict):
        raise DiscoveryPipelineError(
            f"handler returned non-dict: {type(out).__name__}"
        )
    if "status" in out:
        data["status"] = out["status"]
    if "message" in out:
        data["message"] = out["message"]
    status = str(out.get("status") or "").strip().lower()
    if raise_on_failure and status == STATUS_FAILURE:
        raise DiscoveryPipelineError(
            str(out.get("message") or "pipeline task failed")
        )
    return out


def run_discovery_handler(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    impl: Callable[[MutableMapping[str, Any], Any, Any], Any],
    *,
    require_client: bool = True,
) -> Dict[str, Any]:
    """
    Run *impl(data, client, log) -> summary*; map failures to ``DiscoveryPipelineError``.

    *impl* must return a summary dict on success (not ``{"status": "failure"}``).
    """
    from .function_logging import resolve_function_logger

    log: Any = None
    try:
        log = resolve_function_logger(data, None)
        if require_client and not client:
            raise ValueError("CogniteClient is required")
        summary = impl(data, client, log)
        return discovery_handler_success(fn_external_id, data, summary, log=log)
    except DiscoveryPipelineError:
        raise
    except Exception as ex:
        discovery_handler_failure(fn_external_id, data, str(ex), log=log, cause=ex)
