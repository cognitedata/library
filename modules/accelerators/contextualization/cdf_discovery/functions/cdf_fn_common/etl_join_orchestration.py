"""Join stage: two predecessor cohort streams → keyed match → sink RAW or in-memory buffer."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from cdf_fn_common.etl_cohort_storage import (
    canvas_node_id_for_task,
    iter_cohort_entity_rows,
    require_pipeline_run_key,
    resolve_node_cohort_sink,
)
from cdf_fn_common.etl_discovery_cohort import (
    _cohort_row_from_columns,
    _props_from_row_columns,
)
from cdf_fn_common.etl_discovery_query_shared import (
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
    _first_nonempty,
    _flush_rows,
    resolve_query_sink,
    resolve_task_config,
)
from cdf_fn_common.etl_incremental_scope import RAW_ROW_KEY_COLUMN, NODE_INSTANCE_ID_COLUMN
from cdf_fn_common.etl_join_on_eval import eval_join_on
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue
from cdf_fn_common.etl_ui_progress import (
    COHORT_WRITE_ROW_INTERVAL,
    emit_cohort_write_progress_complete,
    emit_cohort_write_progress_every_n_rows,
    set_cohort_write_progress_total,
)
from cdf_fn_common.etl_task_runtime import find_compiled_task, merge_compiled_task_into_data
from cdf_fn_common.etl_common import require_pipeline_run_key


def _join_task_ids_from_data(data: Mapping[str, Any]) -> Tuple[str, str]:
    left = _first_nonempty(data.get("join_left_task_id"))
    right = _first_nonempty(data.get("join_right_task_id"))
    if left and right:
        return left, right
    cw = data.get("compiled_workflow")
    tid = _first_nonempty(data.get("task_id"))
    task = find_compiled_task(cw, task_id=str(tid)) if cw and tid else None
    if isinstance(task, dict):
        payload = task.get("payload")
        if isinstance(payload, dict):
            left = left or _first_nonempty(payload.get("join_left_task_id"))
            right = right or _first_nonempty(payload.get("join_right_task_id"))
    if not left or not right:
        raise ValueError("join task requires join_left_task_id and join_right_task_id in payload")
    return left, right


def validate_join_config(cfg: Mapping[str, Any]) -> None:
    jo = cfg.get("join_on")
    if not isinstance(jo, dict) or not jo:
        raise ValueError("join task requires non-empty config.join_on")
    jt = str(cfg.get("join_type") or "inner").strip().lower()
    if jt not in ("inner", "left"):
        raise ValueError("join_type must be 'inner' or 'left'")


def merge_join_props(
    left_props: Mapping[str, Any],
    right_props: Optional[Mapping[str, Any]],
    right_prefix: str,
) -> Dict[str, Any]:
    out: Dict[str, Any] = copy.deepcopy(dict(left_props))
    if not right_props:
        return out
    px = str(right_prefix or "").strip()
    for k, v in right_props.items():
        if k == "raw_columns" and isinstance(v, Mapping):
            rc = copy.deepcopy(dict(v))
            if px:
                for ck, cv in rc.items():
                    out[f"{px}{ck}"] = cv
            else:
                out["raw_columns"] = rc
            continue
        nk = f"{px}{k}" if px else k
        out[nk] = v
    return out


def _rows_from_task_buffer(data: Mapping[str, Any], task_id: str) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    buffers = data.get("etl_task_row_buffers")
    if not isinstance(buffers, dict):
        return []
    raw = buffers.get(task_id)
    if not isinstance(raw, list):
        return []
    out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for item in raw:
        if isinstance(item, dict) and isinstance(item.get("properties"), dict):
            out.append((dict(item.get("columns") or {}), dict(item["properties"])))
    return out


def _join_rows(
    left_rows: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    right_rows: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    *,
    join_on: Mapping[str, Any],
    join_type: str,
    right_prefix: str,
) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    right_props_list = [props for _cols, props in right_rows]
    out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for cols, left_props in left_rows:
        matched: Optional[Mapping[str, Any]] = None
        for rprops in right_props_list:
            if eval_join_on(left_props, rprops, join_on):
                matched = rprops
                break
        if matched is None:
            if join_type == "left":
                merged = merge_join_props(left_props, None, right_prefix)
                out.append((cols, merged))
            continue
        merged = merge_join_props(left_props, matched, right_prefix)
        out.append((cols, merged))
    return out


def etl_handle_join_in_memory(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    cfg: Dict[str, Any],
    *,
    task_id: str,
    run_id: str,
    log: Any,
) -> Dict[str, Any]:
    left_tid, right_tid = _join_task_ids_from_data(data)
    join_type = str(cfg.get("join_type") or "inner").strip().lower()
    right_prefix = str(cfg.get("right_prefix") or "").strip()
    join_on = cfg.get("join_on")
    if not isinstance(join_on, dict):
        raise ValueError("join_on must be a mapping")

    left_rows = _rows_from_task_buffer(data, left_tid)
    right_rows = _rows_from_task_buffer(data, right_tid)
    if not left_rows:
        raise ValueError(f"join in_memory: no rows buffered for left task {left_tid!r}")
    if not right_rows:
        raise ValueError(f"join in_memory: no rows buffered for right task {right_tid!r}")

    joined = _join_rows(
        left_rows,
        right_rows,
        join_on=join_on,
        join_type=join_type,
        right_prefix=right_prefix,
    )
    out_rows = [{"columns": cols, "properties": props} for cols, props in joined]
    data["_predecessor_rows"] = out_rows

    if log and hasattr(log, "info"):
        log.info(
            "%s join in_memory rows_read_left=%s rows_read_right=%s rows_written=%s",
            fn_external_id,
            len(left_rows),
            len(right_rows),
            len(out_rows),
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read_left": len(left_rows),
        "rows_read_right": len(right_rows),
        "rows_written": len(out_rows),
        "run_id": run_id,
        "status": "ok",
        "predecessor_mode": "in_memory",
        "join_left_task_id": left_tid,
        "join_right_task_id": right_tid,
    }


def etl_handle_join_cohort(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    cfg: Dict[str, Any],
    *,
    task_id: str,
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("cohort join requires a CDF client")

    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    writer_canvas = canvas_node_id_for_task(data, task_id)
    sink_db, sink_table = resolve_query_sink(data)
    join_type = str(cfg.get("join_type") or "inner").strip().lower()
    right_prefix = str(cfg.get("right_prefix") or "").strip()
    join_on = cfg.get("join_on")
    if not isinstance(join_on, dict):
        raise ValueError("join_on must be a mapping")

    left_tid, right_tid = _join_task_ids_from_data(data)
    left_canvas = canvas_node_id_for_task(data, left_tid)
    right_canvas = canvas_node_id_for_task(data, right_tid)
    ldb, ltb = resolve_node_cohort_sink(data, left_tid)
    rdb, rtb = resolve_node_cohort_sink(data, right_tid)

    right_rows: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for row in iter_cohort_entity_rows(client, rdb, rtb):
        cols = dict(getattr(row, "columns", None) or {})
        if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
            continue
        right_rows.append((cols, _props_from_row_columns(cols)))

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_read_left = 0
    rows_read_right = len(right_rows)
    rows_written = 0

    for row in iter_cohort_entity_rows(client, ldb, ltb):
        cols = dict(getattr(row, "columns", None) or {})
        if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
            continue
        rows_read_left += 1
        left_props = _props_from_row_columns(cols)
        matched: Optional[Mapping[str, Any]] = None
        for _rcols, rprops in right_rows:
            if eval_join_on(left_props, rprops, join_on):
                matched = rprops
                break
        if matched is None:
            if join_type == "left":
                merged = merge_join_props(left_props, None, right_prefix)
                pending.append(
                    _cohort_row_from_columns(
                        cols=cols,
                        row_key=_first_nonempty(cols.get(RAW_ROW_KEY_COLUMN), cols.get(NODE_INSTANCE_ID_COLUMN), rows_written),
                        run_id=run_id,
                        canvas_node_id=writer_canvas,
                        properties=merged,
                        query_source="join",
                    )
                )
                rows_written += 1
                emit_cohort_write_progress_every_n_rows(rows_written)
                if len(pending) >= COHORT_WRITE_ROW_INTERVAL:
                    _flush_rows(queue, sink_db, sink_table, pending, client=client)
            continue
        merged = merge_join_props(left_props, matched, right_prefix)
        pending.append(
            _cohort_row_from_columns(
                cols=cols,
                row_key=_first_nonempty(cols.get(RAW_ROW_KEY_COLUMN), cols.get(NODE_INSTANCE_ID_COLUMN), rows_written),
                run_id=run_id,
                canvas_node_id=writer_canvas,
                properties=merged,
                query_source="join",
            )
        )
        rows_written += 1
        emit_cohort_write_progress_every_n_rows(rows_written)
        if len(pending) >= COHORT_WRITE_ROW_INTERVAL:
            _flush_rows(queue, sink_db, sink_table, pending, client=client)

    _flush_rows(queue, sink_db, sink_table, pending, client=client)
    set_cohort_write_progress_total(rows_written)
    emit_cohort_write_progress_complete(rows_written)

    if log and hasattr(log, "info"):
        log.info(
            "%s join cohort rows_read_left=%s rows_read_right=%s rows_written=%s sink=%s/%s",
            fn_external_id,
            rows_read_left,
            rows_read_right,
            rows_written,
            sink_db,
            sink_table,
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "canvas_node_id": writer_canvas,
        "rows_read_left": rows_read_left,
        "rows_read_right": rows_read_right,
        "rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "status": "ok",
        "predecessor_mode": "cohort",
        "join_left_task_id": left_tid,
        "join_right_task_id": right_tid,
        "join_left_canvas_node_id": left_canvas,
        "join_right_canvas_node_id": right_canvas,
    }


def etl_handle_join(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = dict(resolve_task_config(data) or {})
    if not cfg:
        raise ValueError("join task requires non-empty config")
    validate_join_config(cfg)

    if not bool(cfg.get("enabled", True)):
        return {
            "function_external_id": fn_external_id,
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "status": "skipped",
            "rows_read_left": 0,
            "rows_read_right": 0,
            "rows_written": 0,
            "reason": "disabled",
        }

    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id

    if use_in_memory_predecessors(data, cfg):
        summary = etl_handle_join_in_memory(
            fn_external_id, data, cfg, task_id=task_id, run_id=run_id, log=log
        )
    else:
        summary = etl_handle_join_cohort(
            fn_external_id, data, client, cfg, task_id=task_id, log=log
        )
    summary["description"] = _first_nonempty(cfg.get("description"))
    return summary
