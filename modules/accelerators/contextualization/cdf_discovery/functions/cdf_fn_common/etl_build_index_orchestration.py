"""Build inverted-index rows into the task cohort sink (handoff for downstream RAW save)."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping

from cdf_fn_common.etl_build_index.pipeline import resolve_build_index_config
from cdf_fn_common.etl_cohort_storage import canvas_node_id_for_task, require_pipeline_run_key
from cdf_fn_common.etl_common import _first_nonempty
from cdf_fn_common.etl_discovery_cohort import iter_predecessor_raw_locations
from cdf_fn_common.etl_discovery_query_shared import (
    RECORD_KIND_COLUMN,
    _flush_rows,
    resolve_query_sink,
    resolve_task_config,
)
from cdf_fn_common.etl_incremental_scope import RECORD_KIND_INDEX, raw_row_columns
from cdf_fn_common.etl_inverted_index import parse_index_kinds_config
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.etl_ui_progress import (
    COHORT_WRITE_ROW_INTERVAL,
    emit_cohort_write_progress_complete,
    emit_cohort_write_progress_every_n_rows,
    set_cohort_write_progress_total,
)


def _index_rows_from_in_memory(data: Mapping[str, Any]) -> List[Dict[str, Any]]:
    buf = data.get("_predecessor_index_rows")
    if not isinstance(buf, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in buf:
        if not isinstance(item, dict):
            continue
        key = _first_nonempty(item.get("key"))
        cols = item.get("columns")
        if key and isinstance(cols, Mapping):
            out.append({"key": key, "columns": dict(cols)})
    return out


def _iter_index_rows_for_save(
    client: Any,
    data: Mapping[str, Any],
    task_id: str,
) -> List[Dict[str, Any]]:
    if use_in_memory_predecessors(data):
        return _index_rows_from_in_memory(data)
    if client is None:
        return []
    from cdf_fn_common.etl_cohort_storage import (
        iter_cohort_index_rows,
        predecessor_node_table_locations,
    )

    out: List[Dict[str, Any]] = []
    for source_db, source_table in predecessor_node_table_locations(data, task_id):
        for row in iter_cohort_index_rows(client, source_db, source_table):
            cols = dict(raw_row_columns(row))
            if cols.get(RECORD_KIND_COLUMN) != RECORD_KIND_INDEX:
                continue
            key = _first_nonempty(getattr(row, "key", None), cols.get("RAW_ROW_KEY"))
            if key:
                out.append({"key": key, "columns": cols})
    return out


def etl_handle_build_index_in_memory(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    cfg: Dict[str, Any],
    *,
    task_id: str,
    run_id: str,
    handler_id: str,
    handler_cls: Any,
    resolved: Dict[str, Any],
    log: Any,
) -> Dict[str, Any]:
    pending, rows_read, tokens_indexed, entities_seen = handler_cls.collect_postings(
        None,
        data,
        task_id,
        resolved=resolved,
        run_id=run_id,
    )
    writer_canvas = canvas_node_id_for_task(data, task_id)
    raw_rows = handler_cls.build_rows(
        pending,
        resolved=resolved,
        run_id=run_id,
        canvas_node_id=writer_canvas,
    )
    data["_predecessor_index_rows"] = raw_rows
    data.pop("_predecessor_rows", None)
    if log and hasattr(log, "info"):
        log.info(
            "%s build_index in_memory handler=%s rows_read=%s index_rows=%s tokens=%s",
            fn_external_id,
            handler_id,
            rows_read,
            len(raw_rows),
            tokens_indexed,
        )
    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "handler_id": handler_id,
        "rows_read": rows_read,
        "index_rows_written": len(raw_rows),
        "entities": len(entities_seen),
        "tokens_indexed": tokens_indexed,
        "run_id": run_id,
        "status": "ok",
        "predecessor_mode": "in_memory",
    }


def etl_handle_build_index_cohort(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    cfg: Dict[str, Any],
    *,
    task_id: str,
    handler_id: str,
    handler_cls: Any,
    resolved: Dict[str, Any],
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("cohort build_index requires a CDF client")

    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    writer_canvas = canvas_node_id_for_task(data, task_id)
    sink_db, sink_table = resolve_query_sink(data)

    pending, rows_read, tokens_indexed, entities_seen = handler_cls.collect_postings(
        client,
        data,
        task_id,
        resolved=resolved,
        run_id=run_id,
    )
    raw_rows = handler_cls.build_rows(
        pending,
        resolved=resolved,
        run_id=run_id,
        canvas_node_id=writer_canvas,
    )
    set_cohort_write_progress_total(len(raw_rows))

    queue = RawRowsUploadQueue(client)
    pending_flush: List[Dict[str, Any]] = []
    index_rows_written = 0
    for row in raw_rows:
        pending_flush.append(row)
        index_rows_written += 1
        emit_cohort_write_progress_every_n_rows(index_rows_written)
        if len(pending_flush) >= COHORT_WRITE_ROW_INTERVAL:
            _flush_rows(queue, sink_db, sink_table, pending_flush, client=client)
    _flush_rows(queue, sink_db, sink_table, pending_flush, client=client)
    emit_cohort_write_progress_complete(index_rows_written)

    pred_locations = iter_predecessor_raw_locations(data, task_id)
    if log and hasattr(log, "info"):
        log.info(
            "%s build_index cohort handler=%s rows_read=%s index_rows=%s sink=%s/%s",
            fn_external_id,
            handler_id,
            rows_read,
            len(raw_rows),
            sink_db,
            sink_table,
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "handler_id": handler_id,
        "rows_read": rows_read,
        "index_rows_written": len(raw_rows),
        "rows_written": len(raw_rows),
        "entities": len(entities_seen),
        "tokens_indexed": tokens_indexed,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "status": "ok",
        "predecessor_mode": "cohort",
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }


def etl_handle_build_index(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    handler_id, handler_cls, resolved = resolve_build_index_config(cfg)
    index_pairs = parse_index_kinds_config(resolved)
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)

    if not index_pairs:
        return {
            "function_external_id": fn_external_id,
            "task_id": task_id,
            "handler_id": handler_id,
            "status": "skipped",
            "reason": "no_index_kinds_configured",
            "index_rows_written": 0,
            "rows_read": 0,
        }

    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id

    if use_in_memory_predecessors(data):
        return etl_handle_build_index_in_memory(
            fn_external_id,
            data,
            cfg,
            task_id=task_id,
            run_id=run_id,
            handler_id=handler_id,
            handler_cls=handler_cls,
            resolved=resolved,
            log=log,
        )
    return etl_handle_build_index_cohort(
        fn_external_id,
        data,
        client,
        cfg,
        task_id=task_id,
        handler_id=handler_id,
        handler_cls=handler_cls,
        resolved=resolved,
        log=log,
    )


__all__ = [
    "_iter_index_rows_for_save",
    "etl_handle_build_index",
]
