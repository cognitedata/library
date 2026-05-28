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
    resolve_run_id,
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
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    enumeration_summary,
    mark_truncated,
    resolve_read_limit,
)


def etl_handle_query_raw(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    source_db = _first_nonempty(
        cfg.get("source_raw_db"),
        cfg.get("raw_db"),
    )
    source_table = _first_nonempty(
        cfg.get("source_raw_table"),
        cfg.get("source_raw_table_key"),
        cfg.get("raw_table"),
        cfg.get("raw_table_key"),
    )
    if not source_db or not source_table:
        raise ValueError("config.source_raw_db and source_raw_table (or raw_db/raw_table) are required")

    run_id = resolve_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    wanted_run = _first_nonempty(cfg.get("source_run_id"))
    read_limit = resolve_read_limit(cfg)
    filters = parse_etl_filters(cfg)

    rows: list[dict[str, Any]] = []
    n_read = 0
    enum_stats = QueryEnumerationStats()

    if client is not None:
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
    enum_stats.rows_read = n_read
    enum_stats.rows_written = len(rows)
    enum_stats.list_complete = not enum_stats.rows_truncated
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
        },
    )


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_query_raw("fn_etl_raw_query", data, client, log=None)
