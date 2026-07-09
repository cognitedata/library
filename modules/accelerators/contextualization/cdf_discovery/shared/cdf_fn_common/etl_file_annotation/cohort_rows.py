"""Load predecessor cohort rows for file annotation / fan-out inputs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, MutableMapping

from cdf_fn_common.etl_common import iter_predecessor_rows_for_task, iter_rows_from_task_buffer
from cdf_fn_common.etl_predecessor_mode import use_in_memory_predecessors


def _agent_log(hypothesis_id: str, location: str, message: str, data: Mapping[str, Any]) -> None:
    # region agent log
    try:
        payload = {
            "sessionId": "e09635",
            "runId": "pre-fix",
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": dict(data),
            "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
        }
        with Path(
            "/Users/darren.downtain@cognitedata.com/Documents/GitHub/library/.cursor/debug-e09635.log"
        ).open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # endregion


def predecessor_cohort_rows(
    client: Any,
    data: Mapping[str, Any],
    dep_task_id: str,
) -> List[Dict[str, Any]]:
    """Cohort rows from a wired predecessor task (any instance type)."""
    tid = str(dep_task_id or "").strip()
    if not tid:
        return []
    rows: List[Dict[str, Any]] = []
    direct_buffer_rows = iter_rows_from_task_buffer(data, tid)
    _agent_log(
        "H3",
        "cohort_rows.py:predecessor_cohort_rows",
        "predecessor row source check",
        {
            "dep_task_id": tid,
            "use_in_memory_predecessors": bool(use_in_memory_predecessors(data)),
            "direct_buffer_rows_len": len(direct_buffer_rows),
            "has_client": client is not None,
        },
    )
    if client is not None and not use_in_memory_predecessors(data):
        from cdf_fn_common.etl_cohort_storage import (
            canvas_node_id_for_task,
            iter_cohort_entity_rows,
            node_cohort_table_name,
            require_pipeline_run_key,
            resolve_base_cohort_table,
        )
        from cdf_fn_common.etl_discovery_cohort import _props_from_row_columns
        from cdf_fn_common.etl_incremental_scope import raw_row_columns

        raw_db, base_table = resolve_base_cohort_table(data)
        run_id = require_pipeline_run_key(data)
        writer_canvas_node = canvas_node_id_for_task(data, tid)
        raw_table = node_cohort_table_name(base_table, run_id, writer_canvas_node)
        for row in iter_cohort_entity_rows(client, raw_db, raw_table):
            cols = dict(raw_row_columns(row))
            rows.append({"columns": cols, "properties": _props_from_row_columns(cols)})
        _agent_log(
            "H3",
            "cohort_rows.py:predecessor_cohort_rows",
            "rows from dep task output table",
            {
                "dep_task_id": tid,
                "writer_canvas_node": writer_canvas_node,
                "raw_db": raw_db,
                "raw_table": raw_table,
                "rows_len": len(rows),
            },
        )
        return rows
    for cols, props in iter_rows_from_task_buffer(data, tid):
        rows.append({"columns": dict(cols), "properties": dict(props)})
    if not rows:
        # Backward-compatible fallback for older local-run payloads.
        for cols, props in iter_predecessor_rows_for_task(data, tid):
            rows.append({"columns": dict(cols), "properties": dict(props)})
    _agent_log(
        "H3",
        "cohort_rows.py:predecessor_cohort_rows",
        "rows from dep task local buffers",
        {"dep_task_id": tid, "rows_len": len(rows)},
    )
    return rows


def task_id_from_data(data: Mapping[str, Any], key: str) -> str:
    raw = data.get(key)
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    if isinstance(payload, dict):
        nested = payload.get(key)
        if isinstance(nested, str) and nested.strip():
            return nested.strip()
    cfg = data.get("config") if isinstance(data.get("config"), dict) else {}
    if isinstance(cfg, dict):
        alt = cfg.get(key.replace("_task_id", "")) or cfg.get(key)
        if isinstance(alt, str) and alt.strip():
            return alt.strip()
    return ""
