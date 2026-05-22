"""CDF handler: DM view query — cohort-scoped instances.list and RAW sink."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

try:
    from cognite.client import CogniteClient
except ImportError:
    CogniteClient = None  # type: ignore[misc, assignment]

from cdf_fn_common.discovery_handler_result import (
    discovery_handler_failure,
    discovery_handler_success,
    run_discovery_handler,
)
from fn_dm_view_query.engine.orchestration import discovery_handle_view_query


def _impl(data: MutableMapping[str, Any], client: Any, log: Any) -> Dict[str, Any]:
    summary = discovery_handle_view_query("fn_dm_view_query", data, client, log)
    if summary.get("raw_write_error"):
        discovery_handler_failure(
            "fn_dm_view_query",
            data,
            str(summary.get("raw_write_error")),
            log=log,
        )
    if log and hasattr(log, "info"):
        log.info(
            "fn_dm_view_query complete instances=%s view=%s",
            summary.get("instances_written"),
            summary.get("view"),
        )
    return summary


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    return run_discovery_handler("fn_dm_view_query", data, client, _impl)
