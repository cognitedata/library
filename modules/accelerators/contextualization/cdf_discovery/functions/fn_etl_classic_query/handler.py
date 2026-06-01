"""CDF handler: ETL classic CDF query stage."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_common import (
    _first_nonempty,
    merge_compiled_task_into_data,
    require_pipeline_run_key,
    resolve_task_config,
)
from cdf_fn_common.etl_filter_eval import parse_etl_filters, row_passes_filter
from cdf_fn_common.etl_query_recovery import (
    load_query_checkpoint_state,
    save_query_checkpoint_state,
)
from cdf_fn_common.etl_run_scope import (
    incremental_listing_narrowed,
    is_lookup_full_scan,
    resolve_query_scope_mode,
)
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    enumeration_summary,
    list_all_classic_resources,
    mark_truncated,
    resolve_classic_list_limit,
    resolve_run_record_cap,
)


def _classic_external_id(item: Any) -> str:
    for attr in ("external_id", "id"):
        val = getattr(item, attr, None)
        if val is not None and str(val).strip():
            return str(val).strip()
    return ""


def _classic_dump(item: Any) -> Dict[str, Any]:
    if hasattr(item, "dump"):
        d = item.dump()
        return dict(d) if isinstance(d, dict) else {"value": d}
    if hasattr(item, "as_write_dict"):
        d = item.as_write_dict()
        return dict(d) if isinstance(d, dict) else {"value": d}
    return {"repr": repr(item)}


def etl_handle_query_classic(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    lookup_full_scan = is_lookup_full_scan(cfg)
    resource_type = _first_nonempty(cfg.get("resource_type"), "assets")
    list_limit = resolve_classic_list_limit(cfg)
    read_cap = resolve_run_record_cap(data, cfg)
    filters = parse_etl_filters(cfg)
    query_scope_mode = resolve_query_scope_mode(cfg)
    listing_narrowed = incremental_listing_narrowed(data, cfg)

    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    checkpoint = (
        load_query_checkpoint_state(client, data, task_id=task_id)
        if not lookup_full_scan
        else None
    )

    rows: list[dict[str, Any]] = []
    enum_stats = QueryEnumerationStats()

    if client is not None:
        for item in list_all_classic_resources(client, resource_type, limit=list_limit):
            enum_stats.rows_read += 1
            ext_id = _classic_external_id(item)
            if not ext_id:
                continue
            props = _classic_dump(item)
            if not row_passes_filter(props, filters):
                continue
            rows.append(
                {
                    "columns": {"node_instance_id": ext_id, "external_id": ext_id},
                    "properties": props,
                }
            )
            if read_cap > 0 and len(rows) >= read_cap:
                mark_truncated(enum_stats, reason="max_records_per_run")
                if log and hasattr(log, "warning"):
                    log.warning(
                        "%s classic query truncated at max_records_per_run=%s",
                        fn_external_id,
                        read_cap,
                    )
                break

    if checkpoint is not None and checkpoint.rows_completed > 0 and rows:
        rows = rows[checkpoint.rows_completed :]
    data["_predecessor_rows"] = rows
    enum_stats.rows_written = len(rows)
    enum_stats.pages = 1
    enum_stats.list_complete = not enum_stats.rows_truncated
    if checkpoint is not None:
        save_query_checkpoint_state(
            client,
            data,
            task_id=task_id,
            run_id=run_id,
            rows_completed=checkpoint.rows_completed + len(rows),
            is_complete=not enum_stats.rows_truncated,
        )
    return enumeration_summary(
        enum_stats,
        extra={
            "function_external_id": fn_external_id,
            "task_id": task_id,
            "instances_listed": len(rows),
            "instances_written": len(rows),
            "run_id": run_id,
            "resource_type": resource_type,
            "classic_list_limit": list_limit,
            "query_scope_mode": query_scope_mode,
            "effective_scope_mode": "all" if lookup_full_scan else query_scope_mode,
            "listing_narrowed": listing_narrowed,
            "lookup_full_scan": lookup_full_scan,
            "effective_run_cap": read_cap if read_cap > 0 else None,
            "resume_checkpoint_rows": checkpoint.rows_completed if checkpoint is not None else 0,
            "resume_checkpoint_complete": checkpoint.is_complete if checkpoint is not None else False,
        },
    )


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_query_classic("fn_etl_classic_query", data, client, log=None)
