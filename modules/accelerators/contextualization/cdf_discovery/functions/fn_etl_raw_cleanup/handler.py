"""CDF handler: ETL RAW cleanup (post-run cohort table purge)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_common import (
    _as_dict,
    _first_nonempty,
    merge_compiled_task_into_data,
    resolve_run_id,
    resolve_task_config,
)
from cdf_fn_common.etl_raw_purge import run_etl_raw_cleanup_action
from cdf_fn_common.etl_run_retention import DEFAULT_RETENTION_HOURS


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
    cfg = resolve_task_config(data)

    dry_run = bool(data.get("dry_run") or cfg.get("dry_run"))
    if dry_run:
        return {
            "function_external_id": fn_external_id,
            "task_id": task_id,
            "status": "ok",
            "run_id": run_id,
            "dry_run": True,
            "tables_deleted": 0,
        }

    if client is None:
        raise ValueError("raw_cleanup requires a CDF client")

    purge_stale = cfg.get("purge_stale", True)
    if isinstance(purge_stale, str):
        purge_stale = purge_stale.strip().lower() not in ("0", "false", "no")
    rh = cfg.get("retention_hours", DEFAULT_RETENTION_HOURS)
    try:
        retention_hours = float(rh)
    except (TypeError, ValueError):
        retention_hours = DEFAULT_RETENTION_HOURS

    rto = cfg.get("raw_tables")
    raw_tables_override = rto if isinstance(rto, list) else None

    configuration = _as_dict(data.get("configuration"))
    compiled = data.get("compiled_workflow")
    compiled_map = compiled if isinstance(compiled, dict) else None

    cleanup = run_etl_raw_cleanup_action(
        client,
        scope_document=configuration if configuration else data,
        compiled_workflow=compiled_map,
        run_id=run_id,
        raw_tables_override=raw_tables_override,
        dry_run=False,
        retention_hours=retention_hours,
        purge_stale=bool(purge_stale),
    )
    if cleanup.get("error"):
        raise RuntimeError(str(cleanup["error"]))

    tables_deleted = int(cleanup.get("tables_deleted") or 0)
    if log and hasattr(log, "info"):
        log.info(
            "%s run_id=%s tables_deleted=%s purge_stale=%s",
            fn_external_id,
            run_id,
            tables_deleted,
            purge_stale,
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "status": "ok",
        "run_id": run_id,
        "cleanup": cleanup,
        "tables_deleted": tables_deleted,
    }


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_raw_cleanup("fn_etl_raw_cleanup", data, client, log=None)
