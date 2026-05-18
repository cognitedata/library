"""CDF handler: inverted index from discovery predecessor cohort payloads."""

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
from fn_dm_inverted_index.engine.inverted_index import run_inverted_index


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    log: Any = None
    try:
        log = resolve_function_logger(data, None)
        if not client:
            raise ValueError("CogniteClient is required")
        return run_inverted_index(data, client, log)
    except Exception as ex:
        message = f"fn_dm_inverted_index failed: {ex!s}"
        if log:
            log.error(message)
        return {"status": "failure", "message": message}
