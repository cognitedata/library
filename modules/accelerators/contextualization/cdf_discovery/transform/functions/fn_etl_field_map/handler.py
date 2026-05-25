"""CDF handler: ETL field_map stage — cohort property rename/projection."""

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
    resolve_run_id,
    resolve_task_config,
)
from cdf_fn_common.etl_field_map import apply_field_mappings, validate_field_map_config
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data


def etl_handle_field_map(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    if not bool(cfg.get("enabled", True)):
        return {
            "function_external_id": fn_external_id,
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "status": "skipped",
            "rows_read": 0,
            "rows_written": 0,
            "reason": "disabled",
        }

    mappings = validate_field_map_config(cfg)
    run_id = resolve_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)

    rows_read = 0
    rows_written = 0
    kept: list[dict[str, Any]] = []

    for cols, props in iter_predecessor_rows(data):
        rows_read += 1
        mapped = apply_field_mappings(props, mappings)
        kept.append({"columns": cols, "properties": mapped})
        rows_written += 1

    data["_predecessor_rows"] = kept
    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "run_id": run_id,
        "status": "ok",
        "description": _first_nonempty(cfg.get("description")),
        "mapping_count": len(mappings),
    }


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_field_map("fn_etl_field_map", data, client, log=None)
