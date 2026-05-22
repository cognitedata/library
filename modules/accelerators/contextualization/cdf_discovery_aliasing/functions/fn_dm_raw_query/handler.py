"""CDF handler: discovery RAW query stage."""

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

from cdf_fn_common.discovery_handler_result import run_discovery_handler
from fn_dm_raw_query.engine.orchestration import discovery_handle_raw_query


def _impl(data: MutableMapping[str, Any], client: Any, log: Any) -> Dict[str, Any]:
    summary = discovery_handle_raw_query("fn_dm_raw_query", data, client, log)
    if log and hasattr(log, "info"):
        log.info(
            "fn_dm_raw_query complete rows=%s source=%s/%s",
            summary.get("instances_written"),
            summary.get("source_raw_db"),
            summary.get("source_raw_table"),
        )
    return summary


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    return run_discovery_handler("fn_dm_raw_query", data, client, _impl)
