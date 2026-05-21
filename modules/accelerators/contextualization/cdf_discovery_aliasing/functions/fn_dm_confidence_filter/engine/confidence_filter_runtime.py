"""Runtime handler for fn_dm_confidence_filter."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping

from cdf_fn_common.cohort_confidence_value_filter import (
    apply_confidence_value_filter,
    confidence_value_field_from_config,
    validate_confidence_filter_config,
)
from cdf_fn_common.cohort_storage import canvas_node_id_for_task, require_run_id
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
from cdf_fn_common.discovery_validate import _normalize_field_values
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def discovery_handle_confidence_filter(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    if not cfg:
        raise ValueError("confidence_filter task requires non-empty config")
    validate_confidence_filter_config(cfg)

    if not bool(cfg.get("enabled", True)):
        return {
            "function_external_id": fn_external_id,
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "status": "skipped",
            "rows_read": 0,
            "rows_written": 0,
            "rows_excluded": 0,
            "values_pruned": 0,
            "reason": "disabled",
        }

    value_field = confidence_value_field_from_config(cfg)
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
    values_pruned = 0

    for cols, props in iter_predecessor_instance_props(client, data, task_id):
        rows_read += 1
        before = len(
            _normalize_field_values(
                props.get(value_field),
                initial=1.0,
                field=value_field,
                parallel_source=props,
            )
        )
        out_props = apply_confidence_value_filter(props, cfg)
        if out_props is None:
            rows_excluded += 1
            continue
        after = len(
            _normalize_field_values(
                out_props.get(value_field),
                initial=1.0,
                field=value_field,
                parallel_source=out_props,
            )
        )
        values_pruned += max(0, before - after)
        pending.append(
            _cohort_row_from_columns(
                cols=cols,
                row_key=str(rows_read),
                run_id=run_id,
                canvas_node_id=writer_canvas,
                properties=out_props,
                query_source="confidence_filter",
                value_field=value_field,
            )
        )
        rows_written += 1
        if len(pending) >= 500:
            _flush_rows(queue, sink_db, sink_table, pending, client=client)

    _flush_rows(queue, sink_db, sink_table, pending, client=client)

    if log and hasattr(log, "info"):
        log.info(
            "%s confidence_filter rows_read=%s rows_written=%s rows_excluded=%s sink=%s/%s",
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
        "values_pruned": values_pruned,
        "value_field": value_field,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
    }
    return summary
