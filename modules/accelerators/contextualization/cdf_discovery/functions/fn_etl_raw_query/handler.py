"""CDF handler: ETL RAW query stage."""

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
from cdf_fn_common.etl_raw_read import (
    EXTERNAL_ID_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    RUN_ID_COLUMN,
    iter_raw_table_rows_chunked,
    parse_raw_row_properties,
    raw_row_columns,
)
from cdf_fn_common.etl_query_predecessor import (
    raw_query_rows_from_predecessor_buffer,
    resolve_raw_query_source,
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
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    enumeration_summary,
    mark_truncated,
    resolve_run_record_cap,
)


def etl_handle_query_raw(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    lookup_full_scan = is_lookup_full_scan(cfg)
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    read_limit = resolve_run_record_cap(data, cfg)
    filters = parse_etl_filters(cfg)
    query_scope_mode = resolve_query_scope_mode(cfg)
    listing_narrowed = incremental_listing_narrowed(data, cfg)
    checkpoint = (
        load_query_checkpoint_state(client, data, task_id=task_id)
        if not lookup_full_scan
        else None
    )

    source_db = _first_nonempty(cfg.get("source_raw_db"))
    source_table = _first_nonempty(
        cfg.get("source_raw_table"),
        cfg.get("source_raw_table_key"),
    )
    explicit_source = bool(source_db and source_table)
    pred_source = None if explicit_source else resolve_raw_query_source(data, task_id, cfg)
    wanted_run = _first_nonempty(cfg.get("source_run_id"))
    if pred_source is not None:
        source_db, source_table, wanted_run = pred_source

    rows: list[dict[str, Any]] = []
    n_read = 0
    enum_stats = QueryEnumerationStats()

    if not explicit_source and (pred_source is None or client is None):
        rows, n_read = raw_query_rows_from_predecessor_buffer(
            data,
            task_id,
            filters=filters,
            read_limit=read_limit,
        )
        enum_stats.rows_read = n_read
        enum_stats.rows_written = len(rows)
        enum_stats.list_complete = read_limit <= 0 or n_read <= read_limit
    elif not source_db or not source_table:
        raise ValueError("config.source_raw_db and source_raw_table are required")
    elif client is not None:
        for row in iter_raw_table_rows_chunked(client, source_db, source_table):
            cols = raw_row_columns(row)
            if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
                continue
            if wanted_run and str(cols.get(RUN_ID_COLUMN) or "") != wanted_run:
                continue
            props = parse_raw_row_properties(cols)
            if not row_passes_filter(props, filters):
                continue
            n_read += 1
            if read_limit > 0 and n_read > read_limit:
                mark_truncated(enum_stats, reason="read_limit")
                if log and hasattr(log, "warning"):
                    log.warning(
                        "%s RAW query truncated at read_limit=%s",
                        fn_external_id,
                        read_limit,
                    )
                break

            nid = _first_nonempty(cols.get(NODE_INSTANCE_ID_COLUMN), getattr(row, "key", None))
            ext_id = _first_nonempty(cols.get(EXTERNAL_ID_COLUMN), nid, str(n_read))
            rows.append(
                {
                    "columns": {"node_instance_id": str(nid or ext_id), "external_id": ext_id},
                    "properties": props,
                }
            )

    data["_predecessor_rows"] = rows
    if checkpoint is not None and checkpoint.rows_completed > 0 and rows:
        rows = rows[checkpoint.rows_completed :]
        data["_predecessor_rows"] = rows
    enum_stats.rows_read = n_read
    enum_stats.rows_written = len(rows)
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
            "source_raw_db": source_db,
            "source_raw_table": source_table,
            "read_limit": read_limit,
            "query_scope_mode": query_scope_mode,
            "effective_scope_mode": "all" if lookup_full_scan else query_scope_mode,
            "listing_narrowed": listing_narrowed,
            "lookup_full_scan": lookup_full_scan,
            "effective_run_cap": read_limit if read_limit > 0 else None,
            "resume_checkpoint_rows": checkpoint.rows_completed if checkpoint is not None else 0,
            "resume_checkpoint_complete": checkpoint.is_complete if checkpoint is not None else False,
        },
    )


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_query_raw("fn_etl_raw_query", data, client, log=None)
