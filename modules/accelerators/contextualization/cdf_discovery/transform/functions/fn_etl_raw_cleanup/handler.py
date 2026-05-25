"""CDF handler: ETL RAW cleanup (stub)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_common import _first_nonempty, merge_compiled_task_into_data, resolve_run_id


def etl_handle_raw_cleanup(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    run_id = resolve_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "status": "skipped",
        "reason": "raw_cleanup stub — no rows deleted",
        "run_id": run_id,
    }


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_raw_cleanup("fn_etl_raw_cleanup", data, client, log=None)
