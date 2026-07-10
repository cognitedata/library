"""CDF handler: diagram annotation jsonMapping stage."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_json_mapping_orchestration import etl_handle_json_mapping


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_json_mapping("fn_discovery_etl_json_mapping", data, client, log=None)
