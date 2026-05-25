"""CDF handler: build inverted-index rows into the task cohort sink."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_build_index_orchestration import etl_handle_build_index


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_build_index("fn_etl_build_index", data, client, log=None)
