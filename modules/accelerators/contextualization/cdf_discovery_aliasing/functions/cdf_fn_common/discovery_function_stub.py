"""Shared no-op implementation for discovery pipeline Cognite Functions (phase 1 stubs)."""

from __future__ import annotations

import json
from typing import Any, Dict

from cdf_fn_common.function_logging import resolve_function_logger
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def discovery_handle_cdf(fn_external_id: str, data: Dict[str, Any], client: Any) -> Dict[str, Any]:
    """
    Minimal handler body: merge compiled task slice, log, return JSON ``message`` for observability.

    Real query/transform/validation/save logic replaces this per function package.
    """
    log: Any = None
    try:
        log = resolve_function_logger(data, None)
        log.info("%s (discovery stub)", fn_external_id)
        if not client:
            raise ValueError("CogniteClient is required")
        merge_compiled_task_into_data(data)
        msg = json.dumps(
            {
                "stub": True,
                "function_external_id": fn_external_id,
                "task_id": data.get("task_id"),
            }
        )
        data["status"] = "succeeded"
        data["message"] = msg
        return {"status": "succeeded", "message": msg}
    except Exception as ex:
        message = f"{fn_external_id} failed: {ex!s}"
        if log:
            log.error(message)
        return {"status": "failure", "message": message}
