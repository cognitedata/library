"""Transform pipeline: read predecessor RAW rows, apply v1 handlers, write sink RAW."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping

from fn_dm_transform.engine.transform_handlers import (
    resolve_handler_id,
    transform_row_properties,
    validate_transform_config,
)

from cdf_fn_common.discovery_cohort import (
    _cohort_row_from_columns,
    _props_from_row_columns,
    iter_predecessor_raw_locations,
)
from cdf_fn_common.discovery_query_shared import (
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    _first_nonempty,
    _flush_rows,
    resolve_query_sink,
    resolve_run_id,
    resolve_task_config,
)
from cdf_fn_common.incremental_scope import iter_inter_node_raw_rows_for_filter_run
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def discovery_handle_transform(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    if not cfg:
        raise ValueError("transform task requires non-empty config")
    validate_transform_config(cfg)

    if not bool(cfg.get("enabled", True)):
        return {
            "function_external_id": fn_external_id,
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "status": "skipped",
            "rows_read": 0,
            "rows_written": 0,
            "reason": "disabled",
        }

    run_id = resolve_run_id(data)
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    sink_db, sink_table = resolve_query_sink(data)
    filter_run = _first_nonempty(cfg.get("filter_run_id"), run_id)

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_read = 0
    rows_written = 0
    pred_locations = iter_predecessor_raw_locations(data, task_id)

    for source_db, source_table in pred_locations:
        for row in iter_inter_node_raw_rows_for_filter_run(
            client, source_db, source_table, filter_run or ""
        ):
            cols = dict(getattr(row, "columns", None) or {})
            if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
                continue
            rows_read += 1
            props = _props_from_row_columns(cols)
            for out_props in transform_row_properties(props, cfg):
                pending.append(
                    _cohort_row_from_columns(
                        cols=cols,
                        row_key=str(getattr(row, "key", "") or rows_read),
                        run_id=run_id,
                        task_id=task_id,
                        properties=out_props,
                    )
                )
                rows_written += 1
                if len(pending) >= 500:
                    _flush_rows(queue, sink_db, sink_table, pending)

    _flush_rows(queue, sink_db, sink_table, pending)

    if log and hasattr(log, "info"):
        log.info(
            "%s transformed rows_read=%s rows_written=%s handler=%s sink=%s/%s",
            fn_external_id,
            rows_read,
            rows_written,
            resolve_handler_id(cfg),
            sink_db,
            sink_table,
        )

    summary: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "handler_id": resolve_handler_id(cfg),
        "rows_read": rows_read,
        "rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
    data["run_id"] = run_id
    return summary
