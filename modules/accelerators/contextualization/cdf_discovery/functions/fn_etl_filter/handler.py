"""CDF handler: ETL instance filter with {field}_score support."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_common import (
    _first_nonempty,
    iter_predecessor_rows,
    merge_compiled_task_into_data,
    resolve_run_id,
    resolve_task_config,
)
from cdf_fn_common.etl_filter_eval import parse_etl_filters, row_passes_filter, validate_filter_config


def etl_handle_filter(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    validate_filter_config(cfg)
    filters = parse_etl_filters(cfg)
    run_id = resolve_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)

    rows_read = 0
    rows_written = 0
    rows_excluded = 0
    kept: list[dict[str, Any]] = []

    for cols, props in iter_predecessor_rows(data):
        rows_read += 1
        if not row_passes_filter(props, filters):
            rows_excluded += 1
            continue
        kept.append({"columns": cols, "properties": dict(props)})
        rows_written += 1

    data["_predecessor_rows"] = kept
    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_excluded": rows_excluded,
        "run_id": run_id,
    }


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_filter("fn_etl_filter", data, client, log=None)
