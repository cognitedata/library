"""CDF handler: ETL scoring stage."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_common import (
    _first_nonempty,
    iter_predecessor_rows,
    merge_compiled_task_into_data,
    require_pipeline_run_key,
    resolve_task_config,
)
from cdf_fn_common.etl_discovery_cohort import (
    _cohort_row_from_columns,
    iter_predecessor_instance_props,
    iter_predecessor_raw_locations,
)
from cdf_fn_common.etl_discovery_query_shared import _flush_rows, resolve_query_sink
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors
from cdf_fn_common.etl_raw_upload import RawRowsUploadQueue
from cdf_fn_common.etl_score_validate import (
    score_primary_value_field,
    materialize_scoring_rules,
    score_row_properties,
    validate_scoring_config,
)


def etl_handle_score(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    validate_scoring_config(cfg)
    rules = materialize_scoring_rules(cfg)
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)

    rows_read = 0
    rows_written = 0
    if use_in_memory_predecessors(data, cfg):
        out_rows: list[dict[str, Any]] = []
        for cols, props in iter_predecessor_rows(data):
            rows_read += 1
            scored = score_row_properties(props, cfg, rules)
            out_rows.append({"columns": cols, "properties": scored})
            rows_written += 1
        data["_predecessor_rows"] = out_rows
    else:
        if client is None:
            raise ValueError("cohort score requires a CDF client")
        sink_db, sink_table = resolve_query_sink(data)
        value_field = score_primary_value_field(cfg)
        queue = RawRowsUploadQueue(client)
        pending: list[dict[str, Any]] = []
        task_id = _first_nonempty(data.get("task_id"), fn_external_id)
        for cols, props in iter_predecessor_instance_props(client, data, task_id):
            rows_read += 1
            scored = score_row_properties(props, cfg, rules)
            row_key = _first_nonempty(
                cols.get("ROW_KEY"),
                cols.get("node_instance_id"),
                cols.get("external_id"),
                str(rows_read),
            )
            pending.append(
                _cohort_row_from_columns(
                    cols=cols,
                    row_key=row_key,
                    run_id=run_id,
                    canvas_node_id=task_id,
                    properties=scored,
                    query_source="score",
                    value_field=value_field,
                )
            )
            rows_written += 1
            if len(pending) >= 500:
                _flush_rows(queue, sink_db, sink_table, pending, client=client)
        _flush_rows(queue, sink_db, sink_table, pending, client=client)

    summary: Dict[str, Any] = {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "run_id": run_id,
    }
    if not use_in_memory_predecessors(data, cfg):
        summary["raw_db"] = sink_db
        summary["raw_table"] = sink_table
        summary["status"] = "ok"
        summary["predecessor_mode"] = "cohort"
        summary["predecessor_raw_sources"] = [
            {"raw_db": d, "raw_table": t} for d, t in iter_predecessor_raw_locations(data, task_id)
        ]
    return summary


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_score("fn_etl_score", data, client, log=None)
