"""Merge stage: fan-in cohort RAW from multiple predecessors; merge properties per instance."""

from __future__ import annotations

from typing import Any, Dict, List, MutableMapping, Tuple

from cdf_fn_common.discovery_cohort import (
    _cohort_row_from_columns,
    _props_from_row_columns,
    iter_predecessor_raw_locations,
)
from cdf_fn_common.cohort_storage import canvas_node_id_for_task, require_run_id
from cdf_fn_common.discovery_query_shared import (
    _first_nonempty,
    _flush_rows,
    resolve_query_sink,
    resolve_task_config,
)
from cdf_fn_common.discovery_save_apply import (
    _instance_space_and_external_id,
    _iter_entity_rows_for_save,
)
from cdf_fn_common.property_merge import build_merged_props_for_instance, parse_field_policies
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.save_merge import score_cohort_row
from cdf_fn_common.task_runtime import merge_compiled_task_into_data


def validate_merge_config(cfg: Dict[str, Any]) -> None:
    desc = _first_nonempty(cfg.get("description"))
    if not desc:
        raise ValueError("merge config requires non-empty description")
    policies = cfg.get("field_policies") or cfg.get("save_field_policies")
    if not policies:
        raise ValueError("merge config requires field_policies (or save_field_policies)")


def discovery_handle_merge(
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

    run_id = require_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    writer_canvas = canvas_node_id_for_task(data, task_id)
    sink_db, sink_table = resolve_query_sink(data)
    policy_map = parse_field_policies(cfg)

    from collections import defaultdict

    all_rows = _iter_entity_rows_for_save(client, data, task_id)
    grouped: Dict[Tuple[str, str], list] = defaultdict(list)
    for pred_index, cols, props in all_rows:
        inst_space, ext_id = _instance_space_and_external_id(
            cols, cfg=cfg, data=data, props=props
        )
        if not ext_id:
            continue
        key = (inst_space or "", ext_id)
        sc = score_cohort_row(cols, pred_index)
        grouped[key].append((sc, pred_index, props, cols))

    queue = RawRowsUploadQueue(client)
    pending: List[Dict[str, Any]] = []
    rows_written = 0

    for (_space, _ext), scored in grouped.items():
        if not scored:
            continue
        cols = scored[0][3]
        merged = build_merged_props_for_instance(
            [(s, p, props) for s, p, props, _ in scored],
            policy_map,
        )
        if not merged:
            continue
        pending.append(
            _cohort_row_from_columns(
                cols=cols,
                row_key=str(cols.get("externalId") or rows_written),
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
            "%s merge instances=%s rows_written=%s sink=%s/%s",
            fn_external_id,
            len(grouped),
            rows_written,
            sink_db,
            sink_table,
        )

    summary: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "instances_merged": len(grouped),
        "rows_read": len(all_rows),
        "rows_written": rows_written,
        "run_id": run_id,
        "raw_db": sink_db,
        "raw_table": sink_table,
        "predecessor_raw_sources": [{"raw_db": d, "raw_table": t} for d, t in pred_locations],
    }
    return summary
