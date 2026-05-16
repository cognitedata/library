"""CDF handler: discovery RAW cleanup after pipeline sinks complete."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

try:
    from cognite.client import CogniteClient
except ImportError:
    CogniteClient = None  # type: ignore[misc, assignment]

from cdf_fn_common.function_logging import resolve_function_logger
from fn_dm_discovery_raw_cleanup.engine.discovery_raw_cleanup import run_discovery_raw_cleanup


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    log: Any = None
    try:
        log = resolve_function_logger(data, None)
        if not client:
            raise ValueError("CogniteClient is required")
        return run_discovery_raw_cleanup(data, client, log)
    except Exception as ex:
        message = f"fn_dm_discovery_raw_cleanup failed: {ex!s}"
        if log:
            log.error(message)
        return {"status": "failure", "message": message}
