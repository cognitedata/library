"""CDF handler: discovery join stage."""

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
from fn_dm_join.engine.orchestration import discovery_handle_join


def _impl(data: MutableMapping[str, Any], client: Any, log: Any) -> Dict[str, Any]:
    summary = discovery_handle_join("fn_dm_join", data, client, log)
    if log and hasattr(log, "info"):
        log.info("fn_dm_join complete rows_written=%s", summary.get("rows_written"))
    return summary


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    return run_discovery_handler("fn_dm_join", data, client, _impl)
