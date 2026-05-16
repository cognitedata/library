"""CDF handler: RAW rows upsert from discovery payloads (stub)."""

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

from fn_dm_raw_save.engine.orchestration import discovery_handle_raw_save


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    return discovery_handle_raw_save(data, client)
