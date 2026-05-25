"""CDF handler: ETL join stage (two-way row match)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_join_orchestration import etl_handle_join

__all__ = ["etl_handle_join", "handle"]


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_join("fn_etl_join", data, client, log=None)
