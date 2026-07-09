"""Write query-stage rows to RAW cohort tables (local cohort / deployed parity)."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from cdf_fn_common.etl_cohort_storage import canvas_node_id_for_task
from cdf_fn_common.etl_discovery_query_shared import (
    _flush_rows,
    build_entity_cohort_row,
    resolve_query_sink,
)
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue
from cdf_fn_common.etl_ui_progress import (
    COHORT_WRITE_ROW_INTERVAL,
    emit_cohort_write_progress_complete,
    emit_cohort_write_progress_every_n_rows,
    set_cohort_write_progress_total,
)


def _resolve_cohort_write_batch_size(data: Mapping[str, Any]) -> int:
    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    configuration = data.get("configuration") if isinstance(data.get("configuration"), dict) else {}
    params = configuration.get("parameters") if isinstance(configuration.get("parameters"), dict) else {}
    raw = cfg.get("cohort_write_batch_size")
    if raw is None:
        raw = data.get("cohort_write_batch_size")
    if raw is None:
        raw = params.get("cohort_write_batch_size")
    if raw is None:
        return COHORT_WRITE_ROW_INTERVAL
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        return COHORT_WRITE_ROW_INTERVAL
    return parsed if parsed > 0 else COHORT_WRITE_ROW_INTERVAL


def _count_entity_cohort_source_rows(rows: List[Dict[str, Any]]) -> int:
    n = 0
    for item in rows:
        if not isinstance(item, dict):
            continue
        cols = item.get("columns") if isinstance(item.get("columns"), dict) else {}
        if str(cols.get("node_instance_id") or "").strip():
            n += 1
    return n


def write_entity_rows_to_cohort_sink(
    client: Any,
    data: MutableMapping[str, Any],
    *,
    run_id: str,
    scope_key: str,
    task_id: str,
    query_source: str,
    entity_type: str,
    view_space: str,
    view_external_id: str,
    view_version: str,
    rows: List[Dict[str, Any]],
    log: Any = None,
) -> Dict[str, Any]:
    """Flush in-memory ``rows`` (columns + properties) to this task's cohort RAW sink."""
    if client is None:
        raise ValueError("cohort handoff requires a CDF client")
    raw_db, raw_table = resolve_query_sink(data)
    canvas_node_id = canvas_node_id_for_task(data, task_id)
    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    n_written = 0
    write_batch_size = _resolve_cohort_write_batch_size(data)
    value_field = str((data.get("config") or {}).get("value_field") or "aliases")
    set_cohort_write_progress_total(_count_entity_cohort_source_rows(rows))

    for item in rows:
        if not isinstance(item, dict):
            continue
        cols = dict(item.get("columns") or {})
        props = dict(item.get("properties") or {})
        nid = str(cols.get("node_instance_id") or "").strip()
        ext_id = str(cols.get("external_id") or "").strip()
        if not nid:
            continue
        pending.append(
            build_entity_cohort_row(
                run_id=run_id,
                scope_key=scope_key,
                canvas_node_id=canvas_node_id,
                query_source=query_source,
                node_instance_id=nid,
                external_id=ext_id,
                entity_type=entity_type,
                view_space=view_space,
                view_external_id=view_external_id,
                view_version=view_version,
                properties=props,
                value_field=value_field,
            )
        )
        n_written += 1
        if len(pending) >= write_batch_size:
            _flush_rows(queue, raw_db, raw_table, pending, client=client)
            emit_cohort_write_progress_every_n_rows(n_written, interval=write_batch_size)

    _flush_rows(queue, raw_db, raw_table, pending, client=client)
    emit_cohort_write_progress_complete(n_written)
    if log and hasattr(log, "info"):
        log.info(
            "cohort handoff task=%s wrote=%s sink=%s/%s",
            task_id,
            n_written,
            raw_db,
            raw_table,
        )
    return {
        "rows_written": n_written,
        "raw_db": raw_db,
        "raw_table": raw_table,
        "predecessor_mode": "cohort",
    }


def maybe_handoff_predecessor_rows(
    client: Any,
    data: MutableMapping[str, Any],
    *,
    run_id: str,
    scope_key: str,
    task_id: str,
    query_source: str,
    entity_type: str,
    view_space: str,
    view_external_id: str,
    view_version: str,
    rows: List[Dict[str, Any]],
    log: Any = None,
) -> Optional[Dict[str, Any]]:
    """When cohort mode is active, write rows to RAW and clear in-memory buffer."""
    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    if use_in_memory_predecessors(data, cfg):
        data["_predecessor_rows"] = rows
        return None
    summary = write_entity_rows_to_cohort_sink(
        client,
        data,
        run_id=run_id,
        scope_key=scope_key,
        task_id=task_id,
        query_source=query_source,
        entity_type=entity_type,
        view_space=view_space,
        view_external_id=view_external_id,
        view_version=view_version,
        rows=rows,
        log=log,
    )
    data.pop("_predecessor_rows", None)
    return summary
