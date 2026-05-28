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
    resolve_run_id,
    resolve_task_config,
)
from cdf_fn_common.etl_filter_eval import parse_etl_filters, row_passes_filter
from cdf_fn_common.query_enumeration import (
    QueryEnumerationStats,
    enumeration_summary,
    list_all_classic_resources,
    mark_truncated,
    resolve_classic_list_limit,
    resolve_read_limit,
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
    resource_type = _first_nonempty(cfg.get("resource_type"), cfg.get("classic_resource_type"), "assets")
    list_limit = resolve_classic_list_limit(cfg)
    read_cap = resolve_read_limit(cfg)
    filters = parse_etl_filters(cfg)

    run_id = resolve_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)

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
                mark_truncated(enum_stats, reason="read_limit")
                if log and hasattr(log, "warning"):
                    log.warning(
                        "%s classic query truncated at read_limit=%s",
                        fn_external_id,
                        read_cap,
                    )
                break

    data["_predecessor_rows"] = rows
    enum_stats.rows_written = len(rows)
    enum_stats.pages = 1
    enum_stats.list_complete = not enum_stats.rows_truncated
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
        },
    )


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_query_classic("fn_etl_classic_query", data, client, log=None)
