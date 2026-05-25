"""CDF handler: ETL merge stage (N-way property fan-in)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_merge_orchestration import etl_handle_merge

__all__ = ["etl_handle_merge", "handle"]


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_merge("fn_etl_merge", data, client, log=None)
