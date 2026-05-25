"""Merge stage: fan-in cohort RAW or in-memory rows; merge properties per instance."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Mapping, MutableMapping, Tuple

from cdf_fn_common.etl_cohort_storage import (
    canvas_node_id_for_task,
    iter_cohort_entity_rows,
    predecessor_node_table_locations,
    require_run_id,
)
from cdf_fn_common.etl_common import _first_nonempty, iter_predecessor_rows
from cdf_fn_common.etl_discovery_cohort import (
    _cohort_row_from_columns,
    _props_from_row_columns,
    iter_predecessor_raw_locations,
)
from cdf_fn_common.etl_discovery_query_shared import (
    NODE_INSTANCE_ID_COLUMN,
    _flush_rows,
    cohort_instance_space_and_external_id,
    resolve_query_sink,
    resolve_task_config,
)
from cdf_fn_common.etl_incremental_scope import (
    RAW_ROW_KEY_COLUMN,
    RECORD_KIND_COLUMN,
    RECORD_KIND_ENTITY,
)
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.etl_property_merge import build_merged_props_for_instance, parse_field_policies
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue
from cdf_fn_common.etl_save_merge import filter_props_internal, score_cohort_row
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data

_INTERNAL_PROP_KEYS = frozenset({"raw_columns", "_variant_index"})


def validate_merge_config(cfg: Mapping[str, Any]) -> None:
    desc = _first_nonempty(cfg.get("description"))
    if not desc:
        raise ValueError("merge config requires non-empty description")
    policies = cfg.get("field_policies") or cfg.get("save_field_policies")
    if not policies:
        raise ValueError("merge config requires field_policies (or save_field_policies)")


def _iter_entity_rows_for_merge(
    client: Any,
    data: Mapping[str, Any],
    task_id: str,
) -> List[Tuple[int, Dict[str, Any], Dict[str, Any]]]:
    out: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
    for pred_index, (source_db, source_table) in enumerate(
        predecessor_node_table_locations(data, task_id)
    ):
        for row in iter_cohort_entity_rows(client, source_db, source_table):
            cols = dict(getattr(row, "columns", None) or {})
            if cols.get(RECORD_KIND_COLUMN) not in (None, "", RECORD_KIND_ENTITY):
                continue
            props = filter_props_internal(
                _props_from_row_columns(cols),
                _INTERNAL_PROP_KEYS,
            )
            out.append((pred_index, cols, props))
    return out


def _group_scored_rows(
    rows: List[Tuple[int, Dict[str, Any], Dict[str, Any]]],
    *,
    cfg: Mapping[str, Any],
    data: Mapping[str, Any],
) -> Dict[Tuple[str, str], list]:
    grouped: Dict[Tuple[str, str], list] = defaultdict(list)
    for pred_index, cols, props in rows:
        inst_space, ext_id = cohort_instance_space_and_external_id(
            cols, cfg=cfg, data=data, props=props
        )
        if not ext_id:
            continue
        key = (inst_space or "", ext_id)
        sc = score_cohort_row(cols, pred_index)
        grouped[key].append((sc, pred_index, props, cols))
    return grouped


def etl_handle_merge_in_memory(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    cfg: Dict[str, Any],
    *,
    task_id: str,
    run_id: str,
    log: Any,
) -> Dict[str, Any]:
    policy_map = parse_field_policies(cfg)
    flat: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
    for cols, props in iter_predecessor_rows(data):
        flat.append((0, dict(cols), dict(props)))
    grouped = _group_scored_rows(flat, cfg=cfg, data=data)
    out_rows: List[Dict[str, Any]] = []
    rows_written = 0
    for _key, scored in grouped.items():
        if not scored:
            continue
        cols = scored[0][3]
        merged = build_merged_props_for_instance(
            [(s, p, props) for s, p, props, _ in scored],
            policy_map,
        )
        if not merged:
            continue
        out_rows.append({"columns": dict(cols), "properties": merged})
        rows_written += 1
    data["_predecessor_rows"] = out_rows
    if log and hasattr(log, "info"):
        log.info(
            "%s merge in_memory instances=%s rows_written=%s",
            fn_external_id,
            len(grouped),
            rows_written,
        )
    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "instances_merged": len(grouped),
        "rows_read": len(flat),
        "rows_written": rows_written,
        "run_id": run_id,
        "status": "ok",
        "predecessor_mode": "in_memory",
    }


def etl_handle_merge_cohort(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    cfg: Dict[str, Any],
    *,
    task_id: str,
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("cohort merge requires a CDF client")

    run_id = require_run_id(data)
    data["run_id"] = run_id
    writer_canvas = canvas_node_id_for_task(data, task_id)
    sink_db, sink_table = resolve_query_sink(data)
    policy_map = parse_field_policies(cfg)

    all_rows = _iter_entity_rows_for_merge(client, data, task_id)
    grouped = _group_scored_rows(all_rows, cfg=cfg, data=data)

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_written = 0

    for _key, scored in grouped.items():
        if not scored:
            continue
        cols = scored[0][3]
        merged = build_merged_props_for_instance(
            [(s, p, props) for s, p, props, _ in scored],
            policy_map,
        )
        if not merged:
            continue
        row_key = _first_nonempty(cols.get(RAW_ROW_KEY_COLUMN), cols.get(NODE_INSTANCE_ID_COLUMN))
        pending.append(
            _cohort_row_from_columns(
                cols=cols,
                row_key=row_key,
                run_id=run_id,
                canvas_node_id=writer_canvas,
                properties=merged,
                query_source="merge",
            )
        )
        rows_written += 1
        if len(pending) >= 500:
            _flush_rows(queue, sink_db, sink_table, pending, client=client)

    _flush_rows(queue, sink_db, sink_table, pending, client=client)
    pred_locations = iter_predecessor_raw_locations(data, task_id)

    if log and hasattr(log, "info"):
        log.info(
            "%s merge cohort instances=%s rows_written=%s sink=%s/%s",
            fn_external_id,
            len(grouped),
            rows_written,
            sink_db,
            sink_table,
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "instances_merged": len(grouped),
        "rows_read": len(all_rows),
        "rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "status": "ok",
        "predecessor_mode": "cohort",
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }


def etl_handle_merge(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = dict(resolve_task_config(data) or {})
    if not cfg:
        raise ValueError("merge task requires non-empty config")
    validate_merge_config(cfg)

    if not bool(cfg.get("enabled", True)):
        return {
            "function_external_id": fn_external_id,
            "task_id": _first_nonempty(data.get("task_id"), fn_external_id),
            "status": "skipped",
            "rows_read": 0,
            "rows_written": 0,
            "reason": "disabled",
        }

    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    from cdf_fn_common.etl_common import resolve_run_id

    run_id = resolve_run_id(data)
    data["run_id"] = run_id

    if use_in_memory_predecessors(data, cfg):
        summary = etl_handle_merge_in_memory(
            fn_external_id, data, cfg, task_id=task_id, run_id=run_id, log=log
        )
    else:
        summary = etl_handle_merge_cohort(
            fn_external_id, data, client, cfg, task_id=task_id, log=log
        )
    summary["description"] = _first_nonempty(cfg.get("description"))
    return summary
