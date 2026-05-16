"""CDF handler: discovery validation stage (confidence scoring via validation_rules)."""

from __future__ import annotations

import json
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

from fn_dm_validate.engine.validate_runtime import discovery_handle_validate
from cdf_fn_common.function_logging import resolve_function_logger


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    log: Any = None
    try:
        log = resolve_function_logger(data, None)
        if not client:
            raise ValueError("CogniteClient is required")
        summary = discovery_handle_validate("fn_dm_validate", data, client, log)
        msg = json.dumps(summary)
        data["status"] = "succeeded"
        data["message"] = msg
        if log:
            log.info(
                "fn_dm_validate complete rows_written=%s rules=%s",
                summary.get("rows_written"),
                summary.get("rules_applied"),
            )
        return {"status": "succeeded", "message": msg}
    except Exception as ex:
        message = f"fn_dm_validate failed: {ex!s}"
        if log:
            log.error(message)
        return {"status": "failure", "message": message}
