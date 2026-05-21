"""Runtime handler for fn_dm_filter."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping

from cdf_fn_common.cohort_storage import canvas_node_id_for_task, require_run_id
from cdf_fn_common.cohort_filter_eval import parse_cohort_filters
from cdf_fn_common.discovery_cohort import (
    _cohort_row_from_columns,
    iter_predecessor_instance_props,
)
from cdf_fn_common.discovery_query_shared import (
    _first_nonempty,
    _flush_rows,
    resolve_query_sink,
    resolve_task_config,
)
from cdf_fn_common.discovery_row_filter import row_passes_filter, validate_filter_config
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def discovery_handle_filter(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    if not cfg:
        raise ValueError("filter task requires non-empty config")
    validate_filter_config(cfg)

    if not bool(cfg.get("enabled", True)):
        return {
            "function_external_id": fn_external_id,
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "status": "skipped",
            "rows_read": 0,
            "rows_written": 0,
            "rows_excluded": 0,
            "reason": "disabled",
        }

    filters = parse_cohort_filters(cfg)
    run_id = require_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    writer_canvas = canvas_node_id_for_task(data, task_id)
    sink_db, sink_table = resolve_query_sink(data)

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_read = 0
    rows_written = 0
    rows_excluded = 0

    for cols, props in iter_predecessor_instance_props(client, data, task_id):
        rows_read += 1
        if not row_passes_filter(props, filters):
            rows_excluded += 1
            continue
        pending.append(
            _cohort_row_from_columns(
                cols=cols,
                row_key=str(rows_read),
                run_id=run_id,
                canvas_node_id=writer_canvas,
                properties=props,
                query_source="instance_filter",
                value_field="aliases",
            )
        )
        rows_written += 1
        if len(pending) >= 500:
            _flush_rows(queue, sink_db, sink_table, pending, client=client)

    _flush_rows(queue, sink_db, sink_table, pending, client=client)

    if log and hasattr(log, "info"):
        log.info(
            "%s filter rows_read=%s rows_written=%s rows_excluded=%s sink=%s/%s",
            fn_external_id,
            rows_read,
            rows_written,
            rows_excluded,
            sink_db,
            sink_table,
        )

    summary: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "canvas_node_id": writer_canvas,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "rows_excluded": rows_excluded,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "filters": filters,
    }
    return summary
