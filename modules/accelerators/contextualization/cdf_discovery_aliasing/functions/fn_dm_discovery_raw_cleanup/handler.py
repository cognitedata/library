"""CDF handler: discovery RAW cleanup after pipeline sinks complete."""

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
from fn_dm_discovery_raw_cleanup.engine.discovery_raw_cleanup import run_discovery_raw_cleanup


def _impl(data: MutableMapping[str, Any], client: Any, log: Any) -> Dict[str, Any]:
    return run_discovery_raw_cleanup(data, client, log)


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    return run_discovery_handler("fn_dm_discovery_raw_cleanup", data, client, _impl)
