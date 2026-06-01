"""CDF handler: ETL records stream query stage."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_common import _first_nonempty, require_pipeline_run_key
from cdf_fn_common.etl_discovery_query_shared import resolve_task_config
from cdf_fn_common.etl_filter_eval import parse_etl_filters, row_passes_filter
from cdf_fn_common.etl_records_cohort import QUERY_SOURCE_RECORDS, maybe_handoff_record_rows
from cdf_fn_common.etl_query_recovery import (
    load_query_checkpoint_state,
    save_query_checkpoint_state,
)
from cdf_fn_common.etl_run_scope import (
    incremental_listing_narrowed,
    is_lookup_full_scan,
    resolve_query_scope_mode,
)
from cdf_fn_common.etl_streams_records_api import (
    build_records_request_body,
    iter_record_pages,
    record_to_predecessor_row,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    enumeration_summary,
    mark_truncated,
    resolve_run_record_cap,
)


def etl_handle_query_records(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    lookup_full_scan = is_lookup_full_scan(cfg)
    stream_external_id = _first_nonempty(cfg.get("stream_external_id"))
    if not stream_external_id:
        raise ValueError("query_records requires config.stream_external_id")

    read_mode = _first_nonempty(cfg.get("read_mode"), "sync").lower()
    run_record_cap = resolve_run_record_cap(data, cfg)
    filters = parse_etl_filters(cfg)
    query_scope_mode = resolve_query_scope_mode(cfg)
    listing_narrowed = incremental_listing_narrowed(data, cfg)

    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    scope_key = _first_nonempty(cfg.get("scope_key"), f"records:{stream_external_id}")
    checkpoint = (
        load_query_checkpoint_state(client, data, task_id=task_id)
        if not lookup_full_scan
        else None
    )

    rows: list[dict[str, Any]] = []
    enum_stats = QueryEnumerationStats()
    body_base = build_records_request_body(cfg)
    if checkpoint is not None and checkpoint.continuation_token:
        body_base["cursor"] = checkpoint.continuation_token
    continuation_token = ""

    if client is not None:
        for page in iter_record_pages(
            client,
            stream_external_id,
            read_mode=read_mode,
            body_base=body_base,
        ):
            enum_stats.pages += 1
            page_next_cursor = str(page.get("nextCursor") or page.get("next_cursor") or "").strip()
            for rec in page.get("items") or []:
                if not isinstance(rec, dict):
                    continue
                enum_stats.rows_read += 1
                row = record_to_predecessor_row(rec, stream_external_id=stream_external_id)
                props = row.get("properties") or {}
                if not row_passes_filter(props, filters):
                    continue
                rows.append(row)
                if run_record_cap > 0 and len(rows) >= run_record_cap:
                    mark_truncated(enum_stats, reason="max_records_per_run")
                    if log and hasattr(log, "warning"):
                        log.warning(
                            "%s records query truncated at max_records_per_run=%s",
                            fn_external_id,
                            run_record_cap,
                        )
                    break
            if enum_stats.rows_truncated:
                continuation_token = page_next_cursor
                break
            if run_record_cap > 0 and len(rows) >= run_record_cap:
                continuation_token = page_next_cursor
                break

    if checkpoint is not None and checkpoint.rows_completed > 0 and rows:
        rows = rows[checkpoint.rows_completed :]
    cohort_summary = maybe_handoff_record_rows(
        client,
        data,
        run_id=run_id,
        scope_key=scope_key,
        task_id=task_id,
        stream_external_id=stream_external_id,
        rows=rows,
        log=log,
    )
    if cohort_summary is None:
        data["_predecessor_rows"] = rows

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
            continuation_token=continuation_token,
        )
    extra: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "instances_listed": len(rows),
        "instances_written": len(rows),
        "run_id": run_id,
        "stream_external_id": stream_external_id,
        "read_mode": read_mode,
        "query_source": QUERY_SOURCE_RECORDS,
        "query_scope_mode": query_scope_mode,
        "effective_scope_mode": "all" if lookup_full_scan else query_scope_mode,
        "listing_narrowed": listing_narrowed,
        "lookup_full_scan": lookup_full_scan,
        "effective_run_cap": run_record_cap if run_record_cap > 0 else None,
        "resume_checkpoint_rows": checkpoint.rows_completed if checkpoint is not None else 0,
        "resume_checkpoint_complete": checkpoint.is_complete if checkpoint is not None else False,
    }
    if cohort_summary:
        extra["cohort_handoff"] = cohort_summary
    return enumeration_summary(enum_stats, extra=extra)


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_query_records("fn_etl_records_query", data, client, log=None)
