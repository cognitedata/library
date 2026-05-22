"""Post-run RAW cleanup: truncate tables or delete cohort keys for ``run_id``."""

from __future__ import annotations

from typing import Any, Dict, Mapping, MutableMapping

from cdf_fn_common.discovery_handler_result import discovery_handler_failure
from cdf_fn_common.discovery_raw_purge import run_discovery_raw_cleanup_action
from cdf_fn_common.discovery_query_shared import _as_dict, resolve_run_id, resolve_task_config
from cdf_fn_common.run_id_retention import DEFAULT_RETENTION_HOURS
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def run_discovery_raw_cleanup(
    data: MutableMapping[str, Any], client: Any, log: Any
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    run_id = resolve_run_id(data)
    action = str(cfg.get("action") or "delete_run_cohort_keys").strip()
    dry_run = bool(data.get("dry_run") or cfg.get("dry_run"))
    delete_strict = bool(cfg.get("delete_strict_prefix"))
    purge_stale = bool(cfg.get("purge_stale", True))
    rh = cfg.get("retention_hours", DEFAULT_RETENTION_HOURS)
    try:
        retention_hours = float(rh)
    except (TypeError, ValueError):
        retention_hours = DEFAULT_RETENTION_HOURS
    rto = cfg.get("raw_tables")
    raw_tables = rto if isinstance(rto, list) else None

    scope_document: Mapping[str, Any] = _as_dict(data.get("configuration"))
    compiled = data.get("compiled_workflow")
    compiled_map = compiled if isinstance(compiled, dict) else None

    summary = run_discovery_raw_cleanup_action(
        client,
        scope_document=scope_document,
        compiled_workflow=compiled_map,
        run_id=run_id,
        action=action,
        raw_tables_override=raw_tables,
        dry_run=dry_run,
        delete_strict_prefix=delete_strict,
        retention_hours=retention_hours,
        purge_stale=purge_stale,
    )
    if summary.get("error"):
        discovery_handler_failure(
            "fn_dm_discovery_raw_cleanup",
            data,
            str(summary["error"]),
            log=log,
        )
    if log:
        log.info("fn_dm_discovery_raw_cleanup action=%s run_id=%s", action, run_id)
    return {
        "function_external_id": "fn_dm_discovery_raw_cleanup",
        "task_id": data.get("task_id"),
        "cleanup": summary,
    }
