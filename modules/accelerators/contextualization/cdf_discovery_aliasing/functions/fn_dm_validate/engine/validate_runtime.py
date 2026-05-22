"""Runtime handler for fn_dm_validate (keeps heavy imports out of discovery_validate import cycle)."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping

from cdf_fn_common.cohort_storage import (
    canvas_node_id_for_task,
    invalidate_discovery_cohort_row_index_cache,
    require_run_id,
    resolve_base_cohort_table,
)
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
from cdf_fn_common.discovery_record_failure import (
    build_entity_failure_recorder,
    record_entity_processing_failure,
)
from cdf_fn_common.incremental_scope import RAW_ROW_KEY_COLUMN
from cdf_fn_common.discovery_validate import (
    _initial_confidence,
    _normalize_field_values,
    _parse_validate_fields,
    materialize_validation_rules,
    validate_primary_value_field,
    validate_row_properties,
    validate_validation_config,
)
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def discovery_handle_validate(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    if not cfg:
        raise ValueError("validation task requires non-empty config")
    validate_validation_config(cfg)
    rules_raw = materialize_validation_rules(cfg)
    value_field = validate_primary_value_field(cfg)

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
    sink_db, sink_table = resolve_query_sink(data)
    raw_db, base_table = resolve_base_cohort_table(data)
    failure_recorder = build_entity_failure_recorder(
        client, data, raw_db=raw_db, raw_table=base_table, log=log
    )

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_read = 0
    rows_written = 0
    values_scored = 0

    for cols, props in iter_predecessor_instance_props(client, data, task_id):
        rows_read += 1
        row_key = str(cols.get(RAW_ROW_KEY_COLUMN) or rows_read)
        try:
            out_props = validate_row_properties(props, cfg, rules_raw)
            for field in _parse_validate_fields(cfg):
                before = _normalize_field_values(
                    props.get(field),
                    initial=_initial_confidence(cfg),
                    field=field,
                    parallel_source=props,
                )
                after = _normalize_field_values(
                    out_props.get(field),
                    initial=_initial_confidence(cfg),
                    field=field,
                    parallel_source=out_props,
                )
                values_scored += max(len(before), len(after))
            pending.append(
                _cohort_row_from_columns(
                    cols=cols,
                    row_key=row_key,
                    run_id=run_id,
                    canvas_node_id=writer_canvas,
                    properties=out_props,
                    query_source="validate",
                    value_field=value_field,
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
    failure_recorder.flush_fdm(log=log)
    entities_failed = failure_recorder.entities_failed

    if rows_written > 0:
        invalidate_discovery_cohort_row_index_cache(data, sink_db, sink_table)

    if log and hasattr(log, "info"):
        log.info(
            "%s validated rows_read=%s rows_written=%s rules=%s sink=%s/%s",
            fn_external_id,
            rows_read,
            rows_written,
            len(rules_raw),
            sink_db,
            sink_table,
        )

    summary = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "canvas_node_id": writer_canvas,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "entities_failed": entities_failed,
        "entities_processed": max(0, rows_read - entities_failed),
        "values_scored": values_scored,
        "rules_applied": len(rules_raw),
        "confidence_column": True,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
    }
    return summary
