"""Transform pipeline: read predecessor RAW rows, apply v1 handlers, write sink RAW."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping

from fn_dm_transform.engine.transform_handlers import resolve_handler_id
from fn_dm_transform.engine.transform_steps import (
    apply_transform_steps_to_props,
    validate_transform_pipeline_config,
)

from cdf_fn_common.cohort_storage import (
    canvas_node_id_for_task,
    predecessor_canvas_node_ids,
    require_run_id,
    resolve_base_cohort_table,
)
from cdf_fn_common.discovery_cohort import (
    _cohort_row_from_columns,
    iter_predecessor_raw_locations,
)
from cdf_fn_common.transform_cumulative_input import (
    build_transform_table_indexes,
    iter_unique_predecessor_entity_rows,
    resolve_cumulative_input_props,
)
from cdf_fn_common.discovery_query_shared import (
    _first_nonempty,
    _flush_rows,
    resolve_query_sink,
    resolve_task_config,
)
from cdf_fn_common.pipeline_steps import materialize_transform_steps
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
    validate_transform_pipeline_config(cfg)

    if not bool(cfg.get("enabled", True)):
        return {
            "function_external_id": fn_external_id,
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "status": "skipped",
            "rows_read": 0,
            "rows_written": 0,
            "reason": "disabled",
        }

    run_id = require_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    writer_canvas = canvas_node_id_for_task(data, task_id)
    pred_canvas = predecessor_canvas_node_ids(data, task_id)
    sink_db, sink_table = resolve_query_sink(data)
    raw_db, base_table = resolve_base_cohort_table(data)
    _, steps = materialize_transform_steps(cfg)
    handler_summary = resolve_handler_id(steps[0]) if steps else ""

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_read = 0
    rows_written = 0
    pred_locations = iter_predecessor_raw_locations(data, task_id)
    index_cache = data.get("discovery_cohort_row_index_cache")
    table_indexes = build_transform_table_indexes(
        client,
        raw_db=raw_db,
        base_table=base_table,
        run_id=run_id,
        writer_canvas_node_id=writer_canvas,
        predecessor_canvas_node_ids=pred_canvas,
        cfg=cfg,
        index_cache=index_cache,
    )

    for row_key, cols in iter_unique_predecessor_entity_rows(
        client,
        raw_db,
        base_table,
        run_id,
        predecessor_canvas_node_ids=pred_canvas,
        table_indexes=table_indexes,
        index_cache=index_cache,
    ):
        rows_read += 1
        props = resolve_cumulative_input_props(
            client,
            cols,
            writer_canvas_node_id=writer_canvas,
            predecessor_canvas_node_ids=pred_canvas,
            raw_db=raw_db,
            base_table=base_table,
            run_id=run_id,
            cfg=cfg,
            table_indexes=table_indexes,
        )
        for out_props in apply_transform_steps_to_props(props, cfg):
            pending.append(
                _cohort_row_from_columns(
                    cols=cols,
                    row_key=row_key,
                    run_id=run_id,
                    canvas_node_id=writer_canvas,
                    properties=out_props,
                )
            )
            rows_written += 1
            if len(pending) >= 500:
                _flush_rows(queue, sink_db, sink_table, pending, client=client)

    _flush_rows(queue, sink_db, sink_table, pending, client=client)

    if log and hasattr(log, "info"):
        log.info(
            "%s transformed rows_read=%s rows_written=%s steps=%s sink=%s/%s",
            fn_external_id,
            rows_read,
            rows_written,
            len(steps),
            sink_db,
            sink_table,
        )

    summary: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "canvas_node_id": writer_canvas,
        "handler_id": handler_summary,
        "step_count": len(steps),
        "rows_read": rows_read,
        "rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
    return summary
