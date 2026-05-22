"""Shared no-op implementation for discovery pipeline Cognite Functions (phase 1 stubs)."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from cdf_fn_common.discovery_handler_result import run_discovery_handler
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def discovery_handle_cdf(fn_external_id: str, data: Dict[str, Any], client: Any) -> Dict[str, Any]:
    """
    Minimal handler body: merge compiled task slice, log, return JSON ``message`` for observability.

    Real query/transform/validation/save logic replaces this per function package.
    """

    def _impl(d: MutableMapping[str, Any], c: Any, log: Any) -> Dict[str, Any]:
        log.info("%s (discovery stub)", fn_external_id)
        merge_compiled_task_into_data(d)
        return {
            "stub": True,
            "function_external_id": fn_external_id,
            "task_id": d.get("task_id"),
        }

    return run_discovery_handler(fn_external_id, data, client, _impl)
