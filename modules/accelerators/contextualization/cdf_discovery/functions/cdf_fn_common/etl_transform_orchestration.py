"""RAW cohort transform orchestration (deployed workflow parity)."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping

from cdf_fn_common.etl_cohort_storage import (
    canvas_node_id_for_task,
    invalidate_etl_cohort_row_index_cache,
    predecessor_canvas_node_ids,
    require_run_id,
    resolve_base_cohort_table,
)
from cdf_fn_common.etl_discovery_cohort import (
    _cohort_row_from_columns,
    iter_predecessor_raw_locations,
)
from cdf_fn_common.etl_discovery_query_shared import (
    _first_nonempty,
    _flush_rows,
    resolve_query_sink,
    resolve_task_config,
)
from cdf_fn_common.etl_pipeline_steps import materialize_transform_steps
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue
from cdf_fn_common.etl_record_failure import (
    build_entity_failure_recorder,
    record_entity_processing_failure,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.etl_transform.transform_handlers import resolve_handler_id
from cdf_fn_common.etl_transform.transform_steps import (
    apply_transform_steps_to_props,
    validate_transform_pipeline_config,
)
from cdf_fn_common.etl_transform_cumulative_input import (
    INPUT_MODE_CUMULATIVE,
    build_transform_table_indexes,
    iter_unique_predecessor_entity_rows,
    parse_input_mode,
    resolve_cumulative_input_props,
)
from cdf_fn_common.etl_ui_progress import (
    emit_handler_progress_every_n_rows,
    emit_handler_progress_rows_complete,
)


def etl_handle_transform_cohort(
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

    if client is None:
        raise ValueError("cohort transform requires a CDF client")

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
    failure_recorder = build_entity_failure_recorder(
        client, data, raw_db=raw_db, raw_table=base_table, log=log
    )
    pred_locations = iter_predecessor_raw_locations(data, task_id)
    index_cache = data.get("etl_cohort_row_index_cache")
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
        emit_handler_progress_every_n_rows(rows_read)
        try:
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
        except Exception as row_ex:
            record_entity_processing_failure(
                failure_recorder,
                row_key=row_key,
                cols=cols,
                error_message=str(row_ex),
                log=log,
            )

    _flush_rows(queue, sink_db, sink_table, pending, client=client)
    emit_handler_progress_rows_complete(rows_read)
    failure_recorder.flush_fdm(log=log)
    entities_failed = failure_recorder.entities_failed

    if rows_written > 0 and parse_input_mode(cfg) == INPUT_MODE_CUMULATIVE:
        invalidate_etl_cohort_row_index_cache(data, sink_db, sink_table)

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

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "canvas_node_id": writer_canvas,
        "handler_id": handler_summary,
        "step_count": len(steps),
        "rows_read": rows_read,
        "rows_written": rows_written,
        "entities_failed": entities_failed,
        "entities_processed": max(0, rows_read - entities_failed),
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "status": "ok",
        "predecessor_mode": "cohort",
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
