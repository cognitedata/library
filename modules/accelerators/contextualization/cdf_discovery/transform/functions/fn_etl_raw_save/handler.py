"""CDF handler: ETL RAW save stage."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_save_apply import etl_replicate_raw_save


def etl_handle_save_raw(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    return etl_replicate_raw_save(fn_external_id, data, client, log)


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_save_raw("fn_etl_raw_save", data, client, log=None)
