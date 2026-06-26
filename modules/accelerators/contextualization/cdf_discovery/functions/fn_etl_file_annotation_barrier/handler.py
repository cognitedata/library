"""CDF handler: completion barrier for file annotation detect jobs."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_file_annotation_async_orchestration import etl_handle_file_annotation_barrier


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_file_annotation_barrier("fn_etl_file_annotation_barrier", data, client, log=None)
