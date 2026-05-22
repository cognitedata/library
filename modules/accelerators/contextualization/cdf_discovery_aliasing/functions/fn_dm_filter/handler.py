"""CDF handler: discovery instance filter stage."""

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
from fn_dm_filter.engine.filter_runtime import discovery_handle_filter


def _impl(data: MutableMapping[str, Any], client: Any, log: Any) -> Dict[str, Any]:
    summary = discovery_handle_filter("fn_dm_filter", data, client, log)
    if log and hasattr(log, "info"):
        log.info(
            "fn_dm_filter complete rows_written=%s rows_excluded=%s",
            summary.get("rows_written"),
            summary.get("rows_excluded"),
        )
    return summary


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    return run_discovery_handler("fn_dm_filter", data, client, _impl)
