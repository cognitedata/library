"""CDF handler: ETL SQL preview query stage."""

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
from cdf_fn_common.etl_query_recovery import (
    load_query_checkpoint_state,
    save_query_checkpoint_state,
)
from cdf_fn_common.etl_run_scope import (
    incremental_listing_narrowed,
    is_lookup_full_scan,
    resolve_query_scope_mode,
)
from cdf_fn_common.etl_sql_run import resolve_sql_row_external_id, run_sql_preview
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    SQL_PREVIEW_MAX_ROWS,
    enumeration_summary,
    mark_truncated,
    resolve_run_record_cap,
    resolve_sql_row_limit,
)


def etl_handle_query_sql(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    lookup_full_scan = is_lookup_full_scan(cfg)
    query = _first_nonempty(cfg.get("sql_query"), cfg.get("query"))
    if not query:
        raise ValueError("config.sql_query is required for fn_etl_sql_query")

    sql_limit = resolve_sql_row_limit(cfg)
    raw_limit = cfg.get("limit")
    if raw_limit is None:
        raw_limit = cfg.get("batch_size")
    try:
        explicit_int = int(raw_limit) if raw_limit is not None else 0
    except (TypeError, ValueError):
        explicit_int = 0
    convert_to_string = bool(cfg.get("convert_to_string", True))
    timeout_raw = cfg.get("timeout")
    timeout = int(timeout_raw) if timeout_raw is not None and str(timeout_raw).strip() else None
    if timeout is not None:
        timeout = max(1, min(timeout, 240))
    external_id_column = _first_nonempty(cfg.get("external_id_column"))
    query_scope_mode = resolve_query_scope_mode(cfg)
    listing_narrowed = incremental_listing_narrowed(data, cfg)
    run_record_cap = resolve_run_record_cap(data, cfg)

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
    skipped = 0

    if client is not None:
        preview = run_sql_preview(
            client,
            query=query,
            limit=sql_limit,
            convert_to_string=convert_to_string,
            timeout=timeout,
        )
        items = preview.get("items") or []
        if not isinstance(items, list):
            items = []

        enum_stats.rows_read = len(items)
        enum_stats.pages = 1
        enum_stats.list_complete = True
        if explicit_int > 0 and len(items) >= explicit_int:
            mark_truncated(enum_stats, reason="limit")
        elif explicit_int <= 0 and len(items) >= SQL_PREVIEW_MAX_ROWS:
            mark_truncated(enum_stats, reason="sql_preview_max")

        for i, row in enumerate(items):
            if not isinstance(row, dict):
                skipped += 1
                continue
            props = dict(row)
            ext_id = resolve_sql_row_external_id(props, external_id_column)
            if not ext_id:
                ext_id = f"row_{i}"
            nid = ext_id
            rows.append(
                {
                    "columns": {"node_instance_id": nid, "external_id": ext_id},
                    "properties": props,
                }
            )
            if run_record_cap > 0 and len(rows) >= run_record_cap:
                mark_truncated(enum_stats, reason="max_records_per_run")
                break

    if checkpoint is not None and checkpoint.rows_completed > 0 and rows:
        rows = rows[checkpoint.rows_completed :]
    data["_predecessor_rows"] = rows
    enum_stats.rows_written = len(rows)
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
            "rows_skipped": skipped,
            "run_id": run_id,
            "sql_limit": sql_limit,
            "query_scope_mode": query_scope_mode,
            "effective_scope_mode": "all" if lookup_full_scan else query_scope_mode,
            "listing_narrowed": listing_narrowed,
            "lookup_full_scan": lookup_full_scan,
            "effective_run_cap": run_record_cap if run_record_cap > 0 else None,
            "resume_checkpoint_rows": checkpoint.rows_completed if checkpoint is not None else 0,
            "resume_checkpoint_complete": checkpoint.is_complete if checkpoint is not None else False,
        },
    )


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_query_sql("fn_etl_sql_query", data, client, log=None)
