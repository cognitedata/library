"""Join pipeline: two predecessor RAW cohorts → keyed match → sink RAW."""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from cdf_fn_common.discovery_cohort import (
    _cohort_row_from_columns,
    _props_from_row_columns,
    raw_sink_for_dependency_task,
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
from cdf_fn_common.task_runtime import find_compiled_task, merge_compiled_task_into_data

from fn_dm_join.engine.join_on_eval import eval_join_on


def _join_task_ids_from_data(data: Mapping[str, Any]) -> Tuple[str, str]:
    left = _first_nonempty(data.get("join_left_task_id"))
    right = _first_nonempty(data.get("join_right_task_id"))
    if left and right:
        return left, right
    cw = data.get("compiled_workflow")
    tid = _first_nonempty(data.get("task_id"))
    task = find_compiled_task(cw, task_id=str(tid)) if cw and tid else None
    if isinstance(task, dict):
        pl = task.get("payload")
        if isinstance(pl, dict):
            left = left or _first_nonempty(pl.get("join_left_task_id"))
            right = right or _first_nonempty(pl.get("join_right_task_id"))
    if not left or not right:
        raise ValueError("join task requires join_left_task_id and join_right_task_id in payload")
    return left, right


def _validate_join_config(cfg: Mapping[str, Any]) -> None:
    jo = cfg.get("join_on")
    if not isinstance(jo, dict) or not jo:
        raise ValueError("join task requires non-empty config.join_on")
    jt = str(cfg.get("join_type") or "inner").strip().lower()
    if jt not in ("inner", "left"):
        raise ValueError("join_type must be 'inner' or 'left'")


def _merge_props(
    left_props: Mapping[str, Any],
    right_props: Optional[Mapping[str, Any]],
    right_prefix: str,
) -> Dict[str, Any]:
    out: Dict[str, Any] = copy.deepcopy(dict(left_props))
    if not right_props:
        return out
    px = str(right_prefix or "").strip()
    for k, v in right_props.items():
        nk = f"{px}{k}" if px else k
        out[nk] = v
    return out


def discovery_handle_join(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    if not cfg:
        raise ValueError("join task requires non-empty config")
    _validate_join_config(cfg)

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

    run_id = resolve_run_id(data)
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    sink_db, sink_table = resolve_query_sink(data)
    filter_run = _first_nonempty(cfg.get("filter_run_id"), run_id)
    join_type = str(cfg.get("join_type") or "inner").strip().lower()
    right_prefix = str(cfg.get("right_prefix") or "").strip()
    join_on = cfg.get("join_on")

    left_tid, right_tid = _join_task_ids_from_data(data)
    left_loc = raw_sink_for_dependency_task(data, left_tid)
    right_loc = raw_sink_for_dependency_task(data, right_tid)
    if not left_loc:
        raise ValueError(f"Could not resolve RAW sink for join left task {left_tid!r}")
    if not right_loc:
        raise ValueError(f"Could not resolve RAW sink for join right task {right_tid!r}")

    right_rows: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    ldb, ltb = left_loc
    rdb, rtb = right_loc

    for row in iter_inter_node_raw_rows_for_filter_run(client, rdb, rtb, filter_run or ""):
        cols = dict(getattr(row, "columns", None) or {})
        if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
            continue
        right_rows.append((cols, _props_from_row_columns(cols)))

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_read_left = 0
    rows_read_right = len(right_rows)
    rows_written = 0

    for row in iter_inter_node_raw_rows_for_filter_run(client, ldb, ltb, filter_run or ""):
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
                merged = _merge_props(left_props, None, right_prefix)
                pending.append(
                    _cohort_row_from_columns(
                        cols=cols,
                        row_key=str(getattr(row, "key", "") or rows_written),
                        run_id=run_id,
                        task_id=task_id,
                        properties=merged,
                        query_source="join",
                    )
                )
                rows_written += 1
            continue
        merged = _merge_props(left_props, matched, right_prefix)
        pending.append(
            _cohort_row_from_columns(
                cols=cols,
                row_key=str(getattr(row, "key", "") or rows_written),
                run_id=run_id,
                task_id=task_id,
                properties=merged,
                query_source="join",
            )
        )
        rows_written += 1
        if len(pending) >= 500:
            _flush_rows(queue, sink_db, sink_table, pending)

    _flush_rows(queue, sink_db, sink_table, pending)

    if log and hasattr(log, "info"):
        log.info(
            "%s join rows_read_left=%s rows_read_right=%s rows_written=%s sink=%s/%s",
            fn_external_id,
            rows_read_left,
            rows_read_right,
            rows_written,
            sink_db,
            sink_table,
        )

    summary: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read_left": rows_read_left,
        "rows_read_right": rows_read_right,
        "rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "join_left_task_id": left_tid,
        "join_right_task_id": right_tid,
        "predecessor_raw_sources": [
            {"raw_db": ldb, "raw_table": ltb},
            {"raw_db": rdb, "raw_table": rtb},
        ],
    }
    data["run_id"] = run_id
    return summary
